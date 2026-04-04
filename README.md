# Global Rainfall Pipeline

A high-performance pipeline and dashboard for fetching, processing, and analyzing global satellite rainfall data from Google Earth Engine (GEE). Designed for hydrological researchers, this tool provides real-time situational awareness and long-term trend analysis with clinical, scientific precision.

## Key Features

- **Multi-Dataset Support**: Direct integration with CHIRPS (Infrared + Station) and GPM IMERG (Satellite) collections.
- **Scientific Precision**: Built-in ensemble metrics, temporal anomaly detection, and spatial uncertainty mapping.
- **Memory-Safe Extraction**: Automated spatial chunking and asynchronous background processing to bypass GEE memory limits during massive zonal extractions.
- **"1-Click" Export**: Instantly export extracted data to researcher-ready CSV and GeoTIFF formats via dedicated API endpoints.

## Tech Stack

- **Backend**: Python 3.10+, FastAPI
- **Data Engine**: Google Earth Engine (GEE) Python API, Pandas, GeoPandas
- **Server**: Uvicorn, background threading for long-polling tasks
- **Frontend**: Vanilla JavaScript (ES6), HTML5, CSS3, Leaflet.js, Apache ECharts
- **Styling**: "Swiss Blue" (#2563EB) engineering design system (glassmorphism UI)

## Prerequisites

- Python 3.10 or higher
- A registered [Google Earth Engine](https://earthengine.google.com/) account
- `gcloud` CLI (optional, but recommended for Earth Engine authentication)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/global-rainfall-pipeline.git
cd global-rainfall-pipeline
```

### 2. Install Python Dependencies

It is highly recommended to use a virtual environment.

```bash
python -m venv venv

# Activate on Windows:
.\venv\Scripts\activate
# Activate on macOS/Linux:
source venv/bin/activate

# Install the package and dependencies
pip install -e .
```

### 3. Authenticate Google Earth Engine

Before the pipeline can extract data, you must authorize local access to GEE.

```bash
earthengine authenticate
```
Follow the prompts in your browser to log in and generate the authentication token. The pipeline will also attempt lazy-authentication on startup if credentials are not found.

### 4. Start the Development Server

Start the FastAPI application using Uvicorn:

```bash
python -m uvicorn src.api.main:app --reload
```

Open [http://localhost:8000](http://localhost:8000) in your browser to access the dashboard.

## Architecture

### Directory Structure

```text
├── frontend/                 # Vanilla JS, HTML, CSS Dashboard
│   ├── app.js                # Core UI logic, API polling, ECharts/Leaflet init
│   ├── index.html            # Dashboard layout and structure
│   └── styles.css            # Swiss Blue engineering aesthetics
├── src/                      # Backend Python Application
│   ├── api/                  # FastAPI controllers and routers
│   │   └── main.py           # Core endpoints (jobs, export, spatial vis)
│   ├── pipeline/             # Earth Engine extraction logic
│   │   ├── client.py         # GEE logic (backoff retries, spatial chunking)
│   │   ├── processor.py      # Pandas DataFrame data cleaning
│   │   ├── analytics.py      # Statistical and trend calculations
│   │   └── providers/        # Dataset-specific extractions (CHIRPS, GPM)
├── tests/                    # Pytest test suite
├── pyproject.toml            # Dependencies and tool configurations
└── README.md                 # Project Documentation
```

### Request Lifecycle (Zonal Extraction)

1. User uploads a GeoJSON Boundary via the frontend (`/api/v1/extract/zonal`).
2. FastAPI delegates the task to a background thread to prevent `Uvicorn` from blocking.
3. The `GEEClient` analyzes the `ee.Geometry`. If the bounding box is large, `chunk_geometry()` securely splits it into a grid map to prevent GEE OOM exceptions.
4. `CHIRPSProvider` and `GPMProvider` utilize `ThreadPoolExecutor` to fetch temporal/spatial chunks concurrently.
5. The `processor.py` merges and formats the returns into Pandas DataFrames.
6. The frontend polls `/api/jobs/{id}`. Once completed, the UI visualizes the data and activates the `/api/v1/export/csv` endpoint for 1-click download.

### Production Deployment

For production, the application should be deployed via Docker or a robust process manager like Systemd/Supervisor.

#### Docker Deployment (Recommended)

To run the pipeline in a containerized environment:

1. **Create a `Dockerfile`** (if not already present):
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Expose port and run Uvicorn with multiple workers
EXPOSE 8000
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

2. **Build and Run:**
```bash
docker build -t rainfall-pipeline .
# Note: You must mount your Earth Engine credentials into the container
docker run -p 8000:8000 -v ~/.config/earthengine:/root/.config/earthengine rainfall-pipeline
```

#### Nginx Reverse Proxy (VPS)

If deploying to an Ubuntu/Debian server manually:

1. Run the FastAPI application as a systemd service using `gunicorn` with `uvicorn` workers.
2. Setup Nginx to reverse proxy port 80/443 to your local Uvicorn instance (port 8000).
3. Secure the endpoints using certbot/Let's Encrypt (FastAPI CORS should be restricted in production).

## Available Scripts

| Command | Description |
|---------|-------------|
| `python -m uvicorn src.api.main:app --reload` | Start the FastAPI development server |
| `python -m pytest tests/` | Run the complete Pytest test suite |
| `rainfall-pipeline` | Dedicated CLI command hook (if configured via `pip install -e .`) |

## Troubleshooting

### Earth Engine Auth Failures
**Error:** `ee.ee_exception.EEException: Earth Engine client library not authenticated.`
**Fix:** Run `earthengine authenticate` in your terminal. Ensure the generated token file is saved in the correct path (`~/.config/earthengine/credentials`).

### Memory or Timeout Errors
**Error:** `HttpError 400: User memory limit exceeded.`
**Fix:** The backend `chunk_geometry()` utility should catch this, but if your uploaded GeoJSON is extraordinarily massive (e.g., an entire hemisphere at 0.05-degree resolution), you must lower the `chunk_days` parameter in `src/pipeline/client.py` to smaller temporal windows (e.g., 5-10 days).

### Port 8000 is occupied
**Error:** `[Errno 98] Address already in use`
**Fix:** Start Uvicorn on a different port: `python -m uvicorn src.api.main:app --reload --port 8080`.
