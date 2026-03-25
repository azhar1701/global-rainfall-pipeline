import json
import logging
import uuid
import time
import math
import random
import numpy as np
from typing import Optional
from pathlib import Path
import pandas as pd
import ee

from fastapi import FastAPI, UploadFile, Form, HTTPException, File, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

from src.pipeline.auth import authenticate_gee
from src.pipeline.providers.chirps import CHIRPSProvider
from src.pipeline.providers.gpm import GPMProvider
from src.pipeline.processor import process_rainfall_data, fill_missing_reciprocal

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

# Authenticate GEE on startup
@app.on_event("startup")
async def startup_event():
    try:
        authenticate_gee()
        logger.info("GEE Authenticated successfully for API.")
    except Exception as e:
        logger.error(f"Failed to authenticate GEE on startup: {e}")

# In-memory Job Store
JOBS = {}

def execute_pipeline(job_id: str, provider: str, start_date: str, end_date: str, aoi_data: dict):
    try:
        # Extract Geometry (Real Mode)
        if aoi_data.get('type') == 'FeatureCollection':
            features = aoi_data.get('features', [])
            if not features:
                raise ValueError("FeatureCollection is empty.")
            ee_geometry = ee.Geometry(features[0]['geometry'])
        elif aoi_data.get('type') == 'Feature':
            ee_geometry = ee.Geometry(aoi_data['geometry'])
        else:
            ee_geometry = ee.Geometry(aoi_data)

        # Get Providers
        providers_to_run = []
        if provider.lower() == 'both':
            providers_to_run = [('chirps', CHIRPSProvider()), ('gpm', GPMProvider())]
        elif provider.lower() == 'chirps':
            providers_to_run = [('chirps', CHIRPSProvider())]
        elif provider.lower() == 'gpm':
            providers_to_run = [('gpm', GPMProvider())]
        else:
            raise ValueError(f"Unknown provider: {provider}")

        # Fetch Real Data
        merged_df = None
        for p_name, rain_provider in providers_to_run:
            raw_data = rain_provider.get_rainfall_data(ee_geometry, start_date, end_date)
            df = process_rainfall_data(raw_data, timezone="UTC")
            df['date'] = df['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            for col in ['precipitation', 'rolling_avg_7d', 'z_score']:
                if col in df.columns:
                    df[col] = df[col].astype(object).where(pd.notna(df[col]), None)
            if 'is_anomaly' in df.columns:
                df['is_anomaly'] = df['is_anomaly'].astype('boolean')
            
            if provider.lower() == 'both':
                rename_map = {
                    'precipitation': f'precip_{p_name}',
                    'rolling_avg_7d': f'rolling_{p_name}',
                    'is_anomaly': f'anomaly_{p_name}'
                }
                df = df.rename(columns=rename_map)
                df = df.drop(columns=['z_score'], errors='ignore')
                
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on='date', how='outer')

        if merged_df is None:
            JOBS[job_id]["status"] = "completed"
            JOBS[job_id]["result"] = []
            return

        # Clean NaNs in merged df boolean columns
        for col in merged_df.columns:
            if 'anomaly' in col:
                merged_df[col] = merged_df[col].fillna(False).astype(bool)

        results = merged_df.to_dict(orient="records")
        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["result"] = results

    except Exception as e:
        logger.error(f"Job {job_id} failed: {str(e)}")
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["error"] = str(e)


@app.post("/api/jobs")
async def start_pipeline_job(
    background_tasks: BackgroundTasks,
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

        # Launch the synchronous processor in background
        background_tasks.add_task(
            execute_pipeline, 
            job_id, 
            provider, 
            start_date, 
            end_date, 
            aoi_data
        )
        
        return {"job_id": job_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}")
async def fetch_job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found.")
    return JSONResponse(content=JOBS[job_id])


# Map layer tile generating endpoint
@app.post("/api/map-layer")
async def generate_map_layer(
    provider: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    aoi_file: UploadFile = File(...)
):
    content = await aoi_file.read()
    try:
        aoi_data = json.loads(content)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid GeoJSON file.")
        
    try:
        ee.Number(1).getInfo()
    except Exception:
        # Re-attempt auth. If it fails, map overlay will just throw a normal error
        authenticate_gee()
        ee.Number(1).getInfo()
        
    # Build MapId
    # CHIRPS logic
    ee_geometry = ee.Geometry(aoi_data.get('features', [{}])[0].get('geometry') if aoi_data.get('type') == 'FeatureCollection' else aoi_data['geometry'] if aoi_data.get('type') == 'Feature' else aoi_data)
    
    p_name = 'ucsb-chg/chirps/daily'
    band = 'precipitation'
    if provider.lower() == 'gpm' or provider.lower() == 'both':
        p_name = 'NASA/GPM_L3/IMERG_V06'
        band = 'HQprecipitation'
        
    img_col = ee.ImageCollection(p_name).filterDate(start_date, end_date).filterBounds(ee_geometry)
    img_sum = img_col.select(band).sum().clip(ee_geometry)
    
    # Calculate palette
    vis_params = {
        'min': 0,
        'max': 100, # arbitrarily limit color scale max to 100mm
        'palette': ['#000000', '#0000FF', '#00FFFF', '#00FF00', '#00FF00', '#FFFF00', '#FF0000']
    }
    
    map_id = img_sum.getMapId(vis_params)
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
