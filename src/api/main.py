import json
import logging
import uuid
import time
import math
import random
import threading
import numpy as np
from typing import Optional
from pathlib import Path
import pandas as pd
import ee

from fastapi import FastAPI, UploadFile, Form, HTTPException, File, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from concurrent.futures import ThreadPoolExecutor, as_completed
from src.pipeline.auth import authenticate_gee
from src.pipeline.providers.chirps import CHIRPSProvider
from src.pipeline.providers.gpm import GPMProvider
from src.pipeline.processor import process_rainfall_data, fill_missing_reciprocal
from src.pipeline.client import GEEClient
from src.pipeline.analytics import calculate_trend, calculate_ensemble_metrics

logger = logging.getLogger(__name__)

app = FastAPI(title="Global Rainfall Pipeline API")

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global GEE Client
gee_client = None

def get_gee_client():
    global gee_client
    if gee_client is None:
        logger.info("Initializing GEE Client lazily...")
        gee_client = GEEClient()
    return gee_client

# Authenticate GEE on startup
@app.on_event("startup")
async def startup_event():
    logger.info("Starting API server. GEE Client will be initialized on first request.")

# In-memory Job Store
JOBS = {}

def execute_pipeline(job_id: str, provider: str, start_date: str, end_date: str, aoi_data: dict):
    try:
        # Cache context for export
        JOBS[job_id]["_aoi"] = aoi_data
        JOBS[job_id]["_provider_label"] = provider
        JOBS[job_id]["_dates"] = (start_date, end_date)

        # Extract Geometry
        JOBS[job_id]["progress"] = 5
        JOBS[job_id]["stage"] = "parsing_aoi"
        
        if aoi_data.get('type') == 'FeatureCollection':
            ee_geometry = ee.Geometry(aoi_data['features'][0]['geometry'])
        elif aoi_data.get('type') == 'Feature':
            ee_geometry = ee.Geometry(aoi_data['geometry'])
        else:
            ee_geometry = ee.Geometry(aoi_data)

        # Initialize Providers with Shared Client
        JOBS[job_id]["progress"] = 10
        JOBS[job_id]["stage"] = "initializing_providers"
        
        client = get_gee_client()
        
        # Calculate internal fetch range (30-day lookback for SPI/Rolling warm-up)
        original_start_dt = pd.to_datetime(start_date, utc=True)
        fetch_start = (original_start_dt - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        
        provider_map = {
            'chirps': CHIRPSProvider(client=client),
            'gpm': GPMProvider(client=client)
        }
        
        if provider.lower() == 'both':
            selected_providers = [('chirps', provider_map['chirps']), ('gpm', provider_map['gpm'])]
        else:
            p_key = provider.lower()
            if p_key not in provider_map:
                raise ValueError(f"Unknown provider: {provider}")
            selected_providers = [(p_key, provider_map[p_key])]

        JOBS[job_id]["progress"] = 20
        JOBS[job_id]["stage"] = "fetching_data"
        
        def progress_cb(current, total):
            # Calculate progress within the fetching stage (20% to 80%)
            fetch_progress = 20 + int((current / total) * 60)
            JOBS[job_id]["progress"] = fetch_progress
            JOBS[job_id]["stage"] = f"fetching_chunk_{current}_{total}"

        # Concurrent Data Fetching
        results_dfs = {}
        with ThreadPoolExecutor(max_workers=len(selected_providers)) as executor:
            future_to_provider = {
                executor.submit(p_obj.get_rainfall_data, ee_geometry, fetch_start, end_date, progress_callback=progress_cb): p_name 
                for p_name, p_obj in selected_providers
            }
            
            for future in as_completed(future_to_provider):
                p_name = future_to_provider[future]
                try:
                    raw_data = future.result()
                    if not raw_data or not raw_data.get('features'):
                        logger.warning(f"Provider {p_name} returned no images.")
                        continue
                        
                    # Process from fetch_start to allow rolling window warm-up
                    df = process_rainfall_data(raw_data, start_date=fetch_start, end_date=end_date, timezone="UTC")
                    logger.info(f"Provider {p_name} returned {len(df)} rows (with buffer).")
                    
                    df['date_str'] = df['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    # Store original date string before cleaning for merge
                    if provider.lower() == 'both':
                        df = df.rename(columns={
                            'precipitation': f'precip_{p_name}',
                            'rolling_avg_7d': f'rolling_{p_name}',
                            'is_anomaly': f'anomaly_{p_name}',
                            'spi_30': f'spi_30_{p_name}'
                        }).drop(columns=['z_score'], errors='ignore')
                    
                    results_dfs[p_name] = df
                    # Partial progress
                    JOBS[job_id]["progress"] = min(80, JOBS[job_id].get("progress", 20) + (60 // len(selected_providers)))
                    
                except Exception as e:
                    logger.error(f"Provider {p_name} failed: {e}")
                    raise e

        if not results_dfs:
            JOBS[job_id]["status"] = "completed"
            JOBS[job_id]["result"] = []
            JOBS[job_id]["error"] = "No images found for the selected area and date range in Earth Engine."
            return

        # Merging Results
        JOBS[job_id]["stage"] = "merging_results"
        merged_df = None
        for p_name, df in results_dfs.items():
            if merged_df is None:
                merged_df = df
            else:
                # Merge on the datetime object for precision
                merged_df = pd.merge(merged_df, df, on='date', how='outer')

        if merged_df is not None:
            # Sort by date to ensure continuity
            merged_df = merged_df.sort_values('date')
            
            logger.info(f"Buffered DataFrame rows: {len(merged_df)}")

            # CRITICAL: Slice back to original user range after analytical warm-up
            merged_df = merged_df[merged_df['date'] >= original_start_dt].copy()
            
            logger.info(f"Final Merged DataFrame rows: {len(merged_df)}, columns: {list(merged_df.columns)}")

            # Run Trend Analysis on the numeric dataframe before string conversion
            trend_results = {}
            if provider.lower() == 'both':
                for p_name in results_dfs.keys():
                    p_col = f'precip_{p_name}'
                    trend_results[p_name] = calculate_trend(merged_df, column=p_col)
            else:
                target_provider = list(results_dfs.keys())[0]
                trend_results[target_provider] = calculate_trend(merged_df, column='precipitation')
            
            def clean_for_json(obj):
                if isinstance(obj, dict):
                    return {k: clean_for_json(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [clean_for_json(i) for i in obj]
                elif isinstance(obj, float) and (not np.isfinite(obj)):
                    return None
                return obj

            JOBS[job_id]["analytics"] = clean_for_json(trend_results)
            
            # Run Ensemble Analysis if Both providers used
            if provider.lower() == 'both':
                ensemble_data = calculate_ensemble_metrics(merged_df, col_a='precip_chirps', col_b='precip_gpm')
                JOBS[job_id]["ensemble"] = clean_for_json(ensemble_data)
            
            # Reformat date as ISO string for JSON serialization
            merged_df['date'] = merged_df['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Final Cleanup: Convert all NaNs to None for strict JSON compliance
            cleaned_df = merged_df.astype(object).where(pd.notna(merged_df), None)
            JOBS[job_id]["result"] = cleaned_df.to_dict(orient="records")
            # Cache the original DF for export (already formatted dates is fine)
            JOBS[job_id]["_df"] = merged_df
        else:
            JOBS[job_id]["result"] = []

        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["stage"] = "completed"
        JOBS[job_id]["status"] = "completed"

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Job {job_id} failed: {str(e)}\n{error_details}")
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["error"] = f"{str(e)}"


@app.post("/api/jobs")
async def start_pipeline_job(
    provider: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    aoi_file: UploadFile = File(...)
):
    try:
        content = await aoi_file.read()
        try:
            aoi_data = json.loads(content)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid GeoJSON file.")

        try:
            ee.Number(1).getInfo()
        except Exception:
            authenticate_gee()
            ee.Number(1).getInfo()

        job_id = str(uuid.uuid4())
        JOBS[job_id] = {"status": "running"}

        # Launch the synchronous processor in a true daemon background thread
        # This prevents Uvicorn from hanging on "Waiting for background tasks to complete" during reload
        thread = threading.Thread(
            target=execute_pipeline,
            args=(job_id, provider, start_date, end_date, aoi_data),
            daemon=True
        )
        thread.start()
        
        return {"job_id": job_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/jobs/point")
async def start_point_job(
    lat: float = Form(...),
    lon: float = Form(...),
    provider: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...)
):
    try:
        try:
            ee.Number(1).getInfo()
        except Exception:
            authenticate_gee()
            ee.Number(1).getInfo()

        # Create Point Geometry
        ee_geometry = ee.Geometry.Point([lon, lat])
        # Convert to dict format expected by execute_pipeline
        aoi_data = ee_geometry.toGeoJSON()

        job_id = str(uuid.uuid4())
        JOBS[job_id] = {"status": "running", "type": "point", "lat": lat, "lon": lon}

        thread = threading.Thread(
            target=execute_pipeline,
            args=(job_id, provider, start_date, end_date, aoi_data),
            daemon=True
        )
        thread.start()
        
        return {"job_id": job_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}")
async def fetch_job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found.")
    
    # Filter out internal fields (like _df) that are not JSON serializable
    public_status = {k: v for k, v in JOBS[job_id].items() if not k.startswith('_')}
    return JSONResponse(content=public_status)


@app.get("/api/jobs/{job_id}/export")
async def export_job_data(job_id: str):
    if job_id not in JOBS or "_df" not in JOBS[job_id]:
        raise HTTPException(status_code=404, detail="Job data or export not available.")
    
    df = JOBS[job_id]["_df"]
    import io
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv"
    )
    response.headers["Content-Disposition"] = f"attachment; filename=rainfall_export_{job_id}.csv"
    return response


@app.get("/api/jobs/{job_id}/geotiff")
async def export_job_geotiff(job_id: str):
    if job_id not in JOBS or "_aoi" not in JOBS[job_id]:
        raise HTTPException(status_code=404, detail="Job spatial context not available.")
    
    try:
        client = get_gee_client()
        aoi_data = JOBS[job_id]["_aoi"]
        start_date, end_date = JOBS[job_id]["_dates"]
        provider_label = JOBS[job_id]["_provider_label"]
        
        # Recreate geometry
        if aoi_data.get('type') == 'FeatureCollection':
            geom = ee.Geometry(aoi_data['features'][0]['geometry'])
        else:
            geom = ee.Geometry(aoi_data)
            
        # Select dataset (default to CHIRPS if both)
        p_name = 'ucsb-chg/chirps/daily'
        if provider_label.lower() == 'gpm':
            p_name = 'NASA/GPM_L3/IMERG_V07'
            
        img_col = ee.ImageCollection(p_name).filterDate(start_date, end_date).filterBounds(geom)
        img_sum = img_col.select('precipitation').sum().clip(geom)
        
        content = client.download_image(img_sum, geom, scale=5000)
        
        return StreamingResponse(
            iter([content]),
            media_type="image/tiff",
            headers={"Content-Disposition": f"attachment; filename=rainfall_spatial_{job_id}.tif"}
        )
    except Exception as e:
        logger.error(f"GeoTIFF export failed: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/hydrology/watershed")
async def get_watershed(lat: float, lon: float, level: int = 7):
    """
    Returns the HydroSHEDS basin polygon containing the given coordinates.
    Used for automated drainage basin selection.
    """
    try:
        # Ensure GEE is initialized
        try:
            ee.Number(1).getInfo()
        except Exception:
            authenticate_gee()
            
        client = get_gee_client()
        basin_feature = client.get_watershed_polygon(lat, lon, level=level)
        
        if not basin_feature:
            raise HTTPException(status_code=404, detail="No watershed found at these coordinates.")
            
        return JSONResponse(content=basin_feature)
    except Exception as e:
        logger.error(f"Watershed extraction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Map layer tile generating endpoint
@app.post("/api/map-layer")
async def generate_map_layer(
    provider: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    aoi_file: UploadFile = File(...),
    layer_type: str = Form("precipitation")
):
    content = await aoi_file.read()
    try:
        aoi_data = json.loads(content)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid GeoJSON file.")
        
    try:
        ee.Number(1).getInfo()
    except Exception:
        authenticate_gee()
        ee.Number(1).getInfo()
        
    # Extract Geometry
    if aoi_data.get('type') == 'FeatureCollection':
        ee_geometry = ee.Geometry(aoi_data['features'][0]['geometry'])
    elif aoi_data.get('type') == 'Feature':
        ee_geometry = ee.Geometry(aoi_data['geometry'])
    else:
        ee_geometry = ee.Geometry(aoi_data)
    
    # 1. Handle Accuracy Layer request
    if layer_type == "accuracy":
        client = get_gee_client()
        return client.get_accuracy_layer(ee_geometry, start_date, end_date)
    
    # 2. Handle standard Precipitation layer
    p_id = 'UCSB-CHG/CHIRPS/DAILY'
    band = 'precipitation'
    
    if provider.lower() in ['gpm', 'both']:
        p_id = 'NASA/GPM_L3/IMERG_V07'
        # GPM needs mean * 24 for daily rate if looking at raw imerg, 
        # but GPM V07 daily collections often deliver high-res summaries.
        # For map visualization, we use simple mean over period.
        
    img_col = ee.ImageCollection(p_id).filterDate(start_date, end_date).filterBounds(ee_geometry)
    
    # Spatial Vis
    if provider.lower() in ['gpm', 'both']:
        # GPM IMERG is precipitation rate. We mean it over the period.
        img_vis = img_col.select('precipitation').mean().clip(ee_geometry)
        vis_params = {
            'min': 0, 'max': 5, 
            'palette': ['#000033', '#0000FF', '#00FFFF', '#00FF00', '#FFFF00', '#FF0000']
        }
    else:
        # CHIRPS is daily total. we mean it over the period for normalized map.
        img_vis = img_col.select('precipitation').mean().clip(ee_geometry)
        vis_params = {
            'min': 0, 'max': 5,
            'palette': ['#000033', '#0000FF', '#00FFFF', '#00FF00', '#FFFF00', '#FF0000']
        }
    
    map_id = img_vis.getMapId(vis_params)
    return {"url": map_id['tile_fetcher'].url_format}


# Mount Frontend Static Files
frontend_dir = Path(__file__).resolve().parents[2] / "frontend"

if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/")
    async def serve_index():
        return FileResponse(str(frontend_dir / "index.html"))

    @app.get("/{filename}")
    async def serve_static(filename: str):
        file_path = frontend_dir / filename
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(frontend_dir / "index.html"))
else:
    logger.warning("Frontend directory not found. Serving API only.")
