// App Logic for Global Rainfall Pipeline Dashboard

let map;
let geojsonLayer;
let rainfallChart;
let currentData = null;

// Initialize Map and Chart on Load
document.addEventListener('DOMContentLoaded', () => {
    initMap();
    initChart();
    setupFileInput();
    setupForm();
    setupDownload();
});

// Setup Leaflet Map
function initMap() {
    map = L.map('map').setView([0, 0], 2);

    // Add OpenStreetMap tiles (darkened via CSS filter)
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);
}

// Setup Chart.js
function initChart() {
    const ctx = document.getElementById('rainfallChart').getContext('2d');

    Chart.defaults.color = '#94A3B8';
    Chart.defaults.font.family = "'Fira Sans', sans-serif";

    rainfallChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Precipitation (mm)',
                data: [],
                borderColor: '#3B82F6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                borderWidth: 2,
                pointBackgroundColor: '#14B8A6',
                pointBorderColor: '#0B1120',
                pointRadius: 3,
                pointHoverRadius: 6,
                fill: true,
                tension: 0.4
            },
            {
                label: '7-Day Avg (mm)',
                data: [],
                borderColor: '#14B8A6',
                borderWidth: 2,
                borderDash: [5, 5],
                pointRadius: 0,
                fill: false,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#94A3B8', usePointStyle: true, boxWidth: 8 }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.9)',
                    titleColor: '#F8FAFC',
                    bodyColor: '#3B82F6',
                    padding: 12,
                    cornerRadius: 8,
                    displayColors: false,
                    callbacks: {
                        label: function (context) {
                            return `${context.parsed.y.toFixed(2)} mm`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    title: {
                        display: true,
                        text: 'mm / day',
                        color: '#60A5FA'
                    }
                }
            }
        }
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

            const jobData = await response.json();
            const jobId = jobData.job_id;

            let isComplete = false;
            let dots = 0;
            while (!isComplete) {
                // Wait 1.5 seconds before polling
                await new Promise(r => setTimeout(r, 1500));

                const pollResp = await fetch(`/api/jobs/${jobId}`);
                if (!pollResp.ok) throw new Error("Failed to poll server.");

                const statusData = await pollResp.json();

                if (statusData.status === 'failed') {
                    throw new Error(statusData.error || "Job processor failed internally.");
                } else if (statusData.status === 'completed') {
                    currentData = statusData.result;
                    updateVisualizations(currentData);
                    isComplete = true;

                    // Fetch Map Overlay TileLayer from Earth Engine
                    try {
                        btnText.textContent = "Loading Map Overlay...";
                        const mapResp = await fetch('/api/map-layer', {
                            method: 'POST',
                            body: formData
                        });

                        if (mapResp.ok) {
                            const mapData = await mapResp.json();
                            if (mapData.url && mapData.url.length > 5) {
                                if (window.rainfallTileLayer) {
                                    map.removeLayer(window.rainfallTileLayer);
                                }
                                window.rainfallTileLayer = L.tileLayer(mapData.url, {
                                    opacity: 0.7,
                                    maxZoom: 18,
                                    attribution: 'Map Data &copy; Google Earth Engine'
                                }).addTo(map);
                            }
                        }
                    } catch (mapErr) {
                        console.warn('Failed to load map overlay tilelayer:', mapErr);
                    }

                } else {
                    dots = (dots + 1) % 4;
                    btnText.textContent = "Processing" + ".".repeat(dots);
                }
            }
        } catch (error) {
            console.error("Pipeline Error:", error);
            showError(error.message);
        } finally {
            submitBtn.disabled = false;
            btnText.textContent = "Run Pipeline";
            spinner.classList.add('hidden');
        }
    });
}

function updateVisualizations(data) {
    if (!data || data.length === 0) {
        showError("No data returned for the selected criteria.");
        return;
    }

    const isBoth = 'precip_chirps' in data[0];

    // 1. Update Stats
    const validData = isBoth ? data.filter(d => d.precip_chirps !== null) : data.filter(d => d.precipitation !== null);
    const precips = isBoth ? validData.map(d => d.precip_chirps) : validData.map(d => d.precipitation);

    let total = 0;
    let max = 0;
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

    if (anomalies > 0) {
        document.getElementById('stat-anomalies').style.color = '#EF4444';
    } else {
        document.getElementById('stat-anomalies').style.color = '#14B8A6';
    }

    // 2. Update Chart
    rainfallChart.data.labels = data.map(d => formatDate(d.date));

    if (isBoth) {
        rainfallChart.data.datasets = [
            {
                label: 'CHIRPS (mm)',
                data: data.map(d => d.precip_chirps),
                borderColor: '#3B82F6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                borderWidth: 2,
                pointBackgroundColor: data.map(d => d.anomaly_chirps ? '#EF4444' : '#3B82F6'),
                pointRadius: data.map(d => d.anomaly_chirps ? 6 : 3),
                fill: true,
                tension: 0.4,
                pointHoverRadius: 6
            },
            {
                label: 'GPM (mm)',
                data: data.map(d => d.precip_gpm),
                borderColor: '#F97316',
                backgroundColor: 'rgba(249, 115, 22, 0.1)',
                borderWidth: 2,
                pointBackgroundColor: data.map(d => d.anomaly_gpm ? '#EF4444' : '#F97316'),
                pointRadius: data.map(d => d.anomaly_gpm ? 6 : 3),
                fill: true,
                tension: 0.4,
                pointHoverRadius: 6
            }
        ];
    } else {
        rainfallChart.data.datasets = [
            {
                label: 'Precipitation (mm)',
                data: data.map(d => d.precipitation),
                borderColor: '#3B82F6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                borderWidth: 2,
                pointBackgroundColor: data.map(d => d.is_anomaly ? '#EF4444' : '#14B8A6'),
                pointRadius: data.map(d => d.is_anomaly ? 6 : 3),
                fill: true,
                tension: 0.4,
                pointHoverRadius: 6
            },
            {
                label: '7-Day Avg (mm)',
                data: data.map(d => d.rolling_avg_7d !== undefined ? d.rolling_avg_7d : null),
                borderColor: '#14B8A6',
                borderWidth: 2,
                borderDash: [5, 5],
                pointRadius: 0,
                fill: false,
                tension: 0.4,
                pointHoverRadius: 6
            }
        ];
    }

    rainfallChart.update();

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
        if (!currentData || currentData.length === 0) return;

        const isBoth = 'precip_chirps' in currentData[0];
        const headers = isBoth ? ["date", "chirps_precipitation", "gpm_precipitation"] : ["date", "precipitation"];

        const rows = currentData.map(row => {
            const dateStr = formatDateFull(row.date);
            if (isBoth) {
                const c = row.precip_chirps === null ? '' : row.precip_chirps;
                const g = row.precip_gpm === null ? '' : row.precip_gpm;
                return `${dateStr},${c},${g}`;
            } else {
                const p = (row.precipitation === null || isNaN(row.precipitation)) ? '' : row.precipitation;
                return `${dateStr},${p}`;
            }
        });

        const csvContent = "data:text/csv;charset=utf-8," + headers.join(",") + "\n" + rows.join("\n");
        const encodedUri = encodeURI(csvContent);

        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", "rainfall_data_export.csv");
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
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
