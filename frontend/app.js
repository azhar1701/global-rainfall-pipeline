// App Logic for Global Rainfall Pipeline Dashboard

let map;
let geojsonLayer;
let currentData = null;
let chartInstance = null;

// Initialize Map and Chart on Load
document.addEventListener('DOMContentLoaded', () => {
    initMap();
    initChart();
    setupFileInput();
    setupForm();
    setupDownload();
    setupPresets();
});

// Helper to get CSS variable values
function getThemeColor(varName) {
    return getComputedStyle(document.documentElement).getPropertyValue(varName).trim();
}

// Setup Leaflet Map
function initMap() {
    map = L.map('map').setView([0, 0], 2);

    // Add OpenStreetMap tiles (darkened via CSS filter)
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    // Map Click Handler for Point-Sampling
    map.on('click', async (e) => {
        const { lat, lng } = e.latlng;
        handlePointSample(lat, lng);
    });
}

// Setup Chart.js
// Setup Apache ECharts
function initChart() {
    const chartDom = document.getElementById('rainfallChart');
    if (!chartDom) return;
    chartInstance = echarts.init(chartDom, 'dark', { renderer: 'canvas', useDirtyRect: true });

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'cross', label: { backgroundColor: '#1E293B' } }
        },
        grid: {
            left: '4%',
            right: '4%',
            bottom: '10%',
            top: '15%',
            containLabel: true
        },
        xAxis: { type: 'category', boundaryGap: false, data: [] },
        yAxis: { type: 'value', name: 'mm / day', splitLine: { lineStyle: { color: 'rgba(255,255,255,0.05)' } } },
        series: []
    };

    chartInstance.setOption(option);

    // Ensure responsiveness
    window.addEventListener('resize', () => {
        chartInstance && chartInstance.resize();
    });
}

// Handle File Input UI
function setupFileInput() {
    const fileInput = document.getElementById('aoi_file');
    const dropArea = document.getElementById('drop-area');
    const fileMsg = dropArea.querySelector('.file-msg');

    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, () => dropArea.classList.add('is-active'), false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, () => dropArea.classList.remove('is-active'), false);
    });

    dropArea.addEventListener('drop', (e) => {
        const dt = e.dataTransfer;
        const files = dt.files;
        if (files.length) {
            fileInput.files = files;
            handleFileSelect(files[0]);
        }
    });

    fileInput.addEventListener('change', function () {
        if (this.files.length) handleFileSelect(this.files[0]);
    });

    function handleFileSelect(file) {
        fileMsg.textContent = file.name;

        // Preview AOI on Map
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const geojson = JSON.parse(e.target.result);
                if (geojsonLayer) map.removeLayer(geojsonLayer);

                geojsonLayer = L.geoJSON(geojson, {
                    style: {
                        color: '#14B8A6',
                        weight: 2,
                        opacity: 0.8,
                        fillColor: '#14B8A6',
                        fillOpacity: 0.2
                    }
                }).addTo(map);

                map.fitBounds(geojsonLayer.getBounds(), { padding: [20, 20] });
            } catch (err) {
                console.error("Invalid GeoJSON file", err);
                showError("The selected file is not a valid GeoJSON format.");
            }
        };
        reader.readAsText(file);
    }
}

// Handle Form Submission
function setupForm() {
    const form = document.getElementById('pipeline-form');
    const submitBtn = document.getElementById('submit-btn');
    const btnText = submitBtn.querySelector('.btn-text');
    const spinner = submitBtn.querySelector('.spinner');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        hideError();

        // UI Loading State
        submitBtn.disabled = true;
        btnText.textContent = "Processing...";
        spinner.classList.remove('hidden');
        document.getElementById('download-btn').disabled = true;

        const formData = new FormData(form);

        try {
            // Call FastAPI Backend to start Job
            const response = await fetch('/api/jobs', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.detail || "Failed to submit pipeline job.");
            }

            const { job_id } = await response.json();
            await pollJobStatus(job_id, formData);

        } catch (error) {
            console.error("Pipeline Error:", error);
            showError(error.message);
            updateProgress(0, "Error", true);
        } finally {
            submitBtn.disabled = false;
            btnText.textContent = "Run Pipeline";
            spinner.classList.add('hidden');
        }
    });
}

// Map Click Handler
async function handlePointSample(lat, lon) {
    const provider = document.getElementById('provider').value;
    const start_date = document.getElementById('start_date').value;
    const end_date = document.getElementById('end_date').value;

    hideError();
    updateProgress(5, "Contacting GEE...");

    const formData = new FormData();
    formData.append('lat', lat);
    formData.append('lon', lon);
    formData.append('provider', provider);
    formData.append('start_date', start_date);
    formData.append('end_date', end_date);

    try {
        const resp = await fetch('/api/jobs/point', {
            method: 'POST',
            body: formData
        });

        if (!resp.ok) throw new Error("Failed to sample point.");
        const { job_id } = await resp.json();

        // Add Marker
        if (window.pointMarker) map.removeLayer(window.pointMarker);
        window.pointMarker = L.marker([lat, lon]).addTo(map).bindPopup("Sampling Point...").openPopup();

        await pollJobStatus(job_id, null);
    } catch (err) {
        showError(err.message);
    }
}

// Workflow Step Management
const WORKFLOW_STAGE_MAP = {
    'parsing_aoi': { step: 'aoi', done: ['auth'] },
    'initializing_providers': { step: 'aoi', done: ['auth'] },
    'fetching_data': { step: 'fetch', done: ['auth', 'aoi'] },
    'merging_results': { step: 'process', done: ['auth', 'aoi', 'fetch'] },
    'completed': { step: 'done', done: ['auth', 'aoi', 'fetch', 'process', 'analyze'] },
};

function updateWorkflowSteps(stage) {
    const panel = document.getElementById('workflow-panel');
    panel.classList.remove('hidden');

    const steps = panel.querySelectorAll('.workflow-step');
    const connectors = panel.querySelectorAll('.workflow-connector');

    // Reset all
    steps.forEach(s => { s.classList.remove('is-active', 'is-done', 'is-error'); });
    connectors.forEach(c => c.classList.remove('is-done'));

    // Map chunk stages to the fetch step
    let mappedStage = stage;
    if (stage && stage.startsWith('fetching_chunk_')) mappedStage = 'fetching_data';

    const config = WORKFLOW_STAGE_MAP[mappedStage];
    if (!config) {
        // Default: show auth as active
        const authStep = panel.querySelector('[data-step="auth"]');
        if (authStep) authStep.classList.add('is-active');
        return;
    }

    // Mark done steps
    const stepOrder = ['auth', 'aoi', 'fetch', 'process', 'analyze', 'done'];
    config.done.forEach(doneKey => {
        const el = panel.querySelector(`[data-step="${doneKey}"]`);
        if (el) el.classList.add('is-done');
    });

    // Mark done connectors (connectors follow steps)
    const doneIdx = config.done.map(k => stepOrder.indexOf(k));
    connectors.forEach((c, i) => {
        if (doneIdx.includes(i)) c.classList.add('is-done');
    });

    // Mark active step
    const activeEl = panel.querySelector(`[data-step="${config.step}"]`);
    if (activeEl) activeEl.classList.add('is-active');
}

function setWorkflowError() {
    const panel = document.getElementById('workflow-panel');
    const steps = panel.querySelectorAll('.workflow-step');
    // Find the currently active step and mark it as error
    steps.forEach(s => {
        if (s.classList.contains('is-active')) {
            s.classList.remove('is-active');
            s.classList.add('is-error');
        }
    });
}

function setWorkflowComplete() {
    const panel = document.getElementById('workflow-panel');
    const steps = panel.querySelectorAll('.workflow-step');
    const connectors = panel.querySelectorAll('.workflow-connector');
    steps.forEach(s => {
        s.classList.remove('is-active', 'is-error');
        s.classList.add('is-done');
    });
    connectors.forEach(c => c.classList.add('is-done'));
}

// Generalized Polling
async function pollJobStatus(jobId, originalFormData) {
    const progressContainer = document.getElementById('progress-container');
    progressContainer.classList.remove('hidden');

    // Show workflow panel and set initial state
    updateWorkflowSteps(null);

    let isComplete = false;
    while (!isComplete) {
        await new Promise(r => setTimeout(r, 1500));
        const resp = await fetch(`/api/jobs/${jobId}`);
        const statusData = await resp.json();

        if (statusData.status === 'failed') {
            setWorkflowError();
            throw new Error(statusData.error || "Job failed.");
        }

        updateProgress(statusData.progress || 0, statusData.stage || "Processing");
        updateWorkflowSteps(statusData.stage);

        if (statusData.status === 'completed') {
            setWorkflowComplete();
            window.lastJobId = jobId; // Store for export
            currentData = statusData.result;
            updateVisualizations(currentData, statusData.analytics, statusData.error);
            isComplete = true;

            // Load map layer ONLY if we have originalFormData (meaning it's not a point sample)
            if (originalFormData) {
                loadMapOverlay(originalFormData);
            }

            setTimeout(() => progressContainer.classList.add('hidden'), 2000);
        }
    }
}

function updateProgress(percent, stage, isError = false) {
    const inner = document.getElementById('progress-inner');
    const label = document.getElementById('progress-stage');
    const pct = document.getElementById('progress-percent');

    inner.style.width = `${percent}%`;

    let displayStage = stage.toUpperCase().replace(/_/g, ' ');
    if (stage.startsWith('fetching_chunk_')) {
        const parts = stage.split('_');
        const current = parts[2];
        const total = parts[3];
        displayStage = `GATHERING DATA (${current}/${total})`;
    }

    label.textContent = displayStage;
    pct.textContent = `${percent}%`;

    if (isError) {
        inner.style.background = '#EF4444';
    } else {
        inner.style.background = 'var(--primary-color)';
    }
}

async function loadMapOverlay(formData) {
    try {
        const mapResp = await fetch('/api/map-layer', {
            method: 'POST',
            body: formData
        });

        if (mapResp.ok) {
            const mapData = await mapResp.json();
            if (mapData.url) {
                if (window.rainfallTileLayer) map.removeLayer(window.rainfallTileLayer);
                window.rainfallTileLayer = L.tileLayer(mapData.url, {
                    opacity: 0.7,
                    maxZoom: 18,
                    attribution: 'Google Earth Engine'
                }).addTo(map);
            }
        }
    } catch (e) { console.warn(e); }
}

function updateVisualizations(data, analytics, errorMsg) {
    if (errorMsg) {
        showError(errorMsg);
        return;
    }

    if (!data || data.length === 0) {
        showError("No data found for this selection. Try a different date range or location.");
        return;
    }

    const isBoth = 'precip_chirps' in data[0];

    // 1. Update Stats
    const validData = isBoth ? data.filter(d => d.precip_chirps !== null) : data.filter(d => d.precipitation !== null);
    const precips = isBoth ? validData.map(d => d.precip_chirps) : validData.map(d => d.precipitation);

    let total = 0, max = 0;
    if (precips.length > 0) {
        total = precips.reduce((a, b) => a + b, 0);
        max = Math.max(...precips);
    }
    const avg = precips.length > 0 ? total / precips.length : 0;
    const anomalies = isBoth ? data.filter(d => d.anomaly_chirps || d.anomaly_gpm).length : data.filter(d => d.is_anomaly).length;

    document.getElementById('stat-total').textContent = `${total.toFixed(1)} mm`;
    document.getElementById('stat-avg').textContent = `${avg.toFixed(2)} mm`;
    document.getElementById('stat-max').textContent = `${max.toFixed(1)} mm`;
    document.getElementById('stat-anomalies').textContent = anomalies;

    // Trend Annotation in Title
    const chartTitle = document.querySelector('.chart-container .section-title');
    if (analytics) {
        const firstKey = Object.keys(analytics)[0];
        const trend = analytics[firstKey];
        if (trend.status === 'success') {
            const teal = getThemeColor('--color-teal');
            chartTitle.innerHTML = `Time Series Analysis <span style="font-size:0.75rem; color:${teal}">(Trend: ${trend.trend})</span>`;
        }
    }

    // 2. Update ECharts
    const dates = data.map(d => formatDate(d.date));
    let series = [];

    if (isBoth) {
        series = [
            {
                name: 'CHIRPS',
                type: 'line',
                data: data.map(d => d.precip_chirps),
                smooth: true,
                areaStyle: { opacity: 0.1 },
                itemStyle: { color: getThemeColor('--color-primary') },
                markPoint: {
                    data: data.filter(d => d.anomaly_chirps).map(d => ({ coord: [formatDate(d.date), d.precip_chirps], symbol: 'pin', itemStyle: { color: getThemeColor('--color-error') } }))
                }
            },
            {
                name: 'GPM',
                type: 'line',
                data: data.map(d => d.precip_gpm),
                smooth: true,
                areaStyle: { opacity: 0.1 },
                itemStyle: { color: getThemeColor('--color-cta') },
                markPoint: {
                    data: data.filter(d => d.anomaly_gpm).map(d => ({ coord: [formatDate(d.date), d.precip_gpm], symbol: 'pin', itemStyle: { color: getThemeColor('--color-error') } }))
                }
            }
        ];
    } else {
        series = [
            {
                name: 'Precipitation',
                type: 'line',
                data: data.map(d => d.precipitation),
                smooth: true,
                areaStyle: { opacity: 0.1 },
                itemStyle: { color: getThemeColor('--color-primary') },
                markPoint: {
                    data: data.filter(d => d.is_anomaly).map(d => ({ coord: [formatDate(d.date), d.precipitation], symbol: 'pin', itemStyle: { color: getThemeColor('--color-error') } }))
                }
            },
            {
                name: '7-Day Avg',
                type: 'line',
                data: data.map(d => d.rolling_avg_7d),
                lineStyle: { type: 'dashed' },
                itemStyle: { color: getThemeColor('--color-teal') },
                symbol: 'none'
            }
        ];
    }

    chartInstance.setOption({
        legend: {
            data: isBoth ? ['CHIRPS', 'GPM'] : ['Precipitation', '7-Day Avg'],
            top: 0
        },
        xAxis: { data: dates },
        series: series
    });

    // Final resize to ensure it fits the container after data load
    setTimeout(() => chartInstance.resize(), 100);

    // 3. Update Table
    const tbody = document.querySelector('#data-table tbody');
    const thead = document.querySelector('#data-table thead tr');

    if (isBoth) {
        thead.innerHTML = `<th>Date (UTC)</th><th>CHIRPS (mm)</th><th>GPM (mm)</th>`;
    } else {
        thead.innerHTML = `<th>Date (UTC)</th><th>Precipitation (mm)</th>`;
    }

    tbody.innerHTML = '';

    data.forEach(row => {
        const tr = document.createElement('tr');

        const tdDate = document.createElement('td');
        tdDate.className = 'font-data';
        tdDate.textContent = formatDateFull(row.date);
        tr.appendChild(tdDate);

        if (isBoth) {
            const tdChirps = document.createElement('td');
            tdChirps.className = 'font-data';
            tdChirps.innerHTML = (row.precip_chirps === null ? '<span class="text-muted">NaN</span>' : row.precip_chirps.toFixed(3)) + (row.anomaly_chirps ? ' <span style="color:#EF4444">⚠️</span>' : '');

            const tdGpm = document.createElement('td');
            tdGpm.className = 'font-data';
            tdGpm.innerHTML = (row.precip_gpm === null ? '<span class="text-muted">NaN</span>' : row.precip_gpm.toFixed(3)) + (row.anomaly_gpm ? ' <span style="color:#EF4444">⚠️</span>' : '');

            tr.appendChild(tdChirps);
            tr.appendChild(tdGpm);
        } else {
            const tdPrecip = document.createElement('td');
            tdPrecip.className = 'font-data';
            if (row.precipitation === null || isNaN(row.precipitation)) {
                tdPrecip.innerHTML = '<span class="text-muted">NaN</span>';
            } else {
                let val = row.precipitation.toFixed(3);
                if (row.is_anomaly) val += ' <span style="color:#EF4444">⚠️</span>';
                tdPrecip.innerHTML = val;
            }
            tr.appendChild(tdPrecip);
        }

        tbody.appendChild(tr);
    });

    // Enable Download
    document.getElementById('download-btn').disabled = false;
}

function setupDownload() {
    const btn = document.getElementById('download-btn');
    btn.addEventListener('click', () => {
        // Use the new backend export endpoint
        // Extract jobId from recent polling (needs to be stored)
        if (window.lastJobId) {
            window.open(`/api/jobs/${window.lastJobId}/export`, '_blank');
        } else {
            alert("No job data available to export yet.");
        }
    });
}

function showError(msg) {
    const errDiv = document.getElementById('form-error');
    errDiv.textContent = msg;
    errDiv.classList.remove('hidden');
}

function hideError() {
    const errDiv = document.getElementById('form-error');
    errDiv.classList.add('hidden');
}

// Helpers
function formatDate(dateStr) {
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

function formatDateFull(dateStr) {
    const d = new Date(dateStr);
    return d.toISOString().split('T')[0];
}

// Handle Quick Presets (Optimized: Auto-Run enabled)
function setupPresets() {
    const presetBtns = document.querySelectorAll('.btn-preset');
    const startDateInput = document.getElementById('start_date');
    const endDateInput = document.getElementById('end_date');
    const form = document.getElementById('pipeline-form');

    presetBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const days = parseInt(btn.dataset.days);
            const end = new Date();
            const start = new Date();

            if (days >= 365) {
                // Bulk year logic
                start.setFullYear(end.getFullYear() - (days / 365));
            } else {
                start.setDate(end.getDate() - days);
            }

            startDateInput.value = start.toISOString().split('T')[0];
            endDateInput.value = end.toISOString().split('T')[0];

            presetBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // AUTO-RUN if AOI is ready
            const fileInput = document.getElementById('aoi_file');
            if (fileInput.files.length > 0 || window.pointMarker) {
                form.requestSubmit();
            } else {
                // Visual feedback that AOI is needed
                document.getElementById('drop-area').classList.add('is-active');
                setTimeout(() => document.getElementById('drop-area').classList.remove('is-active'), 1000);
            }
        });
    });
}
