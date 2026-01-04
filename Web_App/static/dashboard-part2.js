// dashboard-part2.js - Query Functions and Basic Charts

// ============================================================================
// MONGODB QUERY FUNCTIONS
// ============================================================================

async function runMongoQuery(queryType) {
    const resultDiv = document.getElementById(`mongo${capitalize(queryType)}Result`);
    if (!resultDiv) {
        console.error('Result div not found for:', queryType);
        return;
    }
    
    resultDiv.innerHTML = '<div class="loading">⏳ Loading...</div>';

    let url = '';
    try {
        switch(queryType) {
            case 'revenue':
                const limit = document.getElementById('mongoRevenueLimit')?.value || 10;
                url = `/mongo/analytics/top-zones-by-revenue?limit=${limit}`;
                break;
            case 'trips':
                const tripsLimit = document.getElementById('mongoTripsLimit')?.value || 10;
                url = `/mongo/analytics/top-zones-by-trips?limit=${tripsLimit}`;
                break;
            case 'passengers':
                url = '/mongo/analytics/passenger-distribution';
                break;
            case 'hourly':
                url = '/mongo/analytics/hourly-stats';
                break;
            case 'anomalies':
                url = '/mongo/analytics/anomaly-detection';
                break;
            default:
                throw new Error('Unknown query type');
        }

        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const result = await response.json();
        if (result.error) throw new Error(result.error);
        if (!result.data || !Array.isArray(result.data)) throw new Error('Invalid data format');
        
        performanceData.mongo.push({
            query: queryType,
            time: result.execution_time_ms || 0
        });

        resultDiv.innerHTML = `
            <div class="execution-time">⚡ ${result.execution_time_ms || 0}ms</div>
            <p>📊 Results: ${result.count || 0} records</p>
        `;

        // Render appropriate chart
        if (queryType === 'revenue') renderMongoRevenueChart(result.data);
        else if (queryType === 'trips') renderMongoTripsChart(result.data);
        else if (queryType === 'passengers') renderMongoPassengersChart(result.data);
        else if (queryType === 'hourly') renderMongoHourlyChart(result.data);
        else if (queryType === 'anomalies') renderAnomaliesVisualization(result.data);

        updateComparisonChart();
    } catch (error) {
        console.error('Query error:', error);
        resultDiv.innerHTML = `<div style="color: red;">❌ Error: ${error.message}</div>`;
    }
}

// ============================================================================
// CASSANDRA QUERY FUNCTIONS
// ============================================================================

async function runCassandraQuery(queryType) {
    const resultDiv = document.getElementById(`cassandra${capitalize(queryType)}Result`);
    if (!resultDiv) {
        console.error('Result div not found for:', queryType);
        return;
    }
    
    resultDiv.innerHTML = '<div class="loading">⏳ Loading...</div>';

    let url = '';
    try {
        if (queryType === 'revenue') {
            const borough = document.getElementById('cassandraBorough')?.value;
            const yearMonth = document.getElementById('cassandraYearMonth')?.value;
            if (!borough || !yearMonth) throw new Error('Borough and year-month required');
            url = `/cassandra/analytics/top-zones-revenue?borough=${encodeURIComponent(borough)}&year_month=${encodeURIComponent(yearMonth)}`;
        } else if (queryType === 'hourly') {
            const borough = document.getElementById('cassandraHourBorough')?.value;
            const yearMonth = document.getElementById('cassandraHourYearMonth')?.value;
            if (!borough || !yearMonth) throw new Error('Borough and year-month required');
            url = `/cassandra/analytics/hourly-performance?borough=${encodeURIComponent(borough)}&year_month=${encodeURIComponent(yearMonth)}`;
        } else if (queryType === 'payment') {
            const borough = document.getElementById('cassandraPaymentBorough')?.value;
            const yearMonth = document.getElementById('cassandraPaymentYearMonth')?.value;
            if (!borough || !yearMonth) throw new Error('Borough and year-month required');
            url = `/cassandra/analytics/payment-types?borough=${encodeURIComponent(borough)}&year_month=${encodeURIComponent(yearMonth)}`;
        }

        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const result = await response.json();
        if (result.error || !result.success) throw new Error(result.error || 'Query failed');
        if (!result.data || !Array.isArray(result.data)) throw new Error('Invalid data format');
        
        performanceData.cassandra.push({
            query: queryType,
            time: result.execution_time_ms || 0
        });

        resultDiv.innerHTML = `
            <div class="execution-time">⚡ ${result.execution_time_ms || 0}ms</div>
            <p>📊 Results: ${result.count || 0} records</p>
        `;

        // Render appropriate chart
        if (queryType === 'revenue') renderCassandraRevenueChart(result.data);
        else if (queryType === 'hourly') renderCassandraHourlyChart(result.data);
        else if (queryType === 'payment') renderCassandraPaymentChart(result.data);

        updateComparisonChart();
        updateThroughputComparisonChart();
    } catch (error) {
        console.error('Query error:', error);
        resultDiv.innerHTML = `<div style="color: red;">❌ Error: ${error.message}</div>`;
    }
}

// ============================================================================
// MONGO CHARTS
// ============================================================================

function renderMongoRevenueChart(data) {
    destroyChart('mongoRevenue');
    const ctx = document.getElementById('mongoRevenueChart');
    if (!ctx || !data || data.length === 0) return;
    
    charts.mongoRevenue = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: data.map(d => d._id || 'Unknown'),
            datasets: [{
                label: 'Total Revenue ($)',
                data: data.map(d => d.total_revenue || 0),
                backgroundColor: 'rgba(19, 170, 82, 0.8)',
                borderColor: 'rgba(19, 170, 82, 1)',
                borderWidth: 2
            }]
        },
        options: getChartOptions('Revenue ($)')
    });
}

function renderMongoTripsChart(data) {
    destroyChart('mongoTrips');
    const ctx = document.getElementById('mongoTripsChart');
    if (!ctx || !data || data.length === 0) return;
    
    charts.mongoTrips = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: data.map(d => d._id || 'Unknown'),
            datasets: [{
                label: 'Total Trips',
                data: data.map(d => d.total_trips || 0),
                backgroundColor: 'rgba(118, 75, 162, 0.8)',
                borderColor: 'rgba(118, 75, 162, 1)',
                borderWidth: 2
            }]
        },
        options: getChartOptions('Number of Trips')
    });
}

function renderMongoPassengersChart(data) {
    destroyChart('mongoPassengers');
    const ctx = document.getElementById('mongoPassengersChart');
    if (!ctx || !data || data.length === 0) return;
    
    charts.mongoPassengers = new Chart(ctx.getContext('2d'), {
        type: 'pie',
        data: {
            labels: data.map(d => `${d._id || 0} passenger(s)`),
            datasets: [{
                data: data.map(d => d.count || 0),
                backgroundColor: [
                    '#667eea', '#764ba2', '#f093fb', '#4facfe',
                    '#43e97b', '#fa709a', '#fee140', '#30cfd0'
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'right' }
            }
        }
    });
}

function renderMongoHourlyChart(data) {
    destroyChart('mongoHourly');
    const ctx = document.getElementById('mongoHourlyChart');
    if (!ctx || !data || data.length === 0) return;
    
    charts.mongoHourly = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
            labels: data.map(d => `${d._id}:00`),
            datasets: [{
                label: 'Revenue ($)',
                data: data.map(d => d.total_revenue || 0),
                borderColor: '#13aa52',
                backgroundColor: 'rgba(19, 170, 82, 0.2)',
                tension: 0.4,
                fill: true,
                yAxisID: 'y'
            }, {
                label: 'Trips',
                data: data.map(d => d.total_trips || 0),
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.2)',
                tension: 0.4,
                fill: true,
                yAxisID: 'y1'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    type: 'linear',
                    position: 'left',
                    title: { display: true, text: 'Revenue ($)' }
                },
                y1: {
                    type: 'linear',
                    position: 'right',
                    title: { display: true, text: 'Number of Trips' },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}

console.log('✅ Dashboard Part 2 initialized successfully');
// ============================================================================
// STANDALONE SHARD DISTRIBUTION VISUALIZATION
// Add this entire block to your dashboard JavaScript file
// No modifications to existing code needed
// ============================================================================

let shardDistributionChart = null;
let boroughByShardChart = null;

/**
 * Main function to run shard distribution analysis
 * Called directly from the HTML button: onclick="analyzeShardDistribution()"
 */
async function analyzeShardDistribution() {
    const resultDiv = document.getElementById('mongoShardDistributionResult');
    const tableDiv = document.getElementById('shardDistributionTable');
    
    // Show loading state
    resultDiv.innerHTML = '<div class="loading">⏳ Analyzing shard distribution...</div>';
    if (tableDiv) {
        tableDiv.innerHTML = '';
    }
    
    try {
        // Fetch data from backend
        const response = await fetch('/mongo/analytics/shard-distribution-detailed');

        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (!data.success) {
            throw new Error(data.error || 'Failed to fetch shard distribution');
        }
        
        // Display results
        displayShardDistributionResults(data);
        createShardDistributionTable(data);
        createShardDistributionCharts(data);
        
    } catch (error) {
        console.error('Shard distribution error:', error);
        resultDiv.innerHTML = `
            <div class="error">
                <strong>Error:</strong> ${error.message}
                <br><small>Make sure MongoDB sharding is enabled and the backend endpoint is available.</small>
            </div>
        `;
    }
}

/**
 * Display summary results
 */
function displayShardDistributionResults(data) {
    const resultDiv = document.getElementById('mongoShardDistributionResult');

    const totalDocs = data.total_documents || 0;
    const numShards = data.total_shards || (data.shards ? data.shards.length : 0);
    const executionTime = data.execution_time_ms || 0;

    resultDiv.innerHTML = `
        <div class="execution-time">
            Execution time: ${executionTime.toFixed(2)} ms
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:15px;margin-top:15px;">
            <div class="metric-card">
                <div class="metric-label">TOTAL DOCUMENTS</div>
                <div class="metric-value">${totalDocs.toLocaleString()}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">TOTAL SHARDS</div>
                <div class="metric-value">${numShards}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">AVG DOCS / SHARD</div>
                <div class="metric-value">
                    ${numShards > 0 ? Math.round(totalDocs / numShards).toLocaleString() : 0}
                </div>
            </div>
        </div>
    `;
}


/**
 * Create detailed table with shard information
 */
function createShardDistributionTable(data) {
    const tableDiv = document.getElementById('shardDistributionTable');
    if (!tableDiv) return;

    const shards = data.shards || [];
    const boroughTotals = data.borough_totals || {};

    // Correct sort field
    const sortedShards = [...shards].sort(
        (a, b) => b.documents - a.documents
    );

    let tableHTML = `
        <h4 style="margin-top:30px;">Shard Details</h4>
        <table>
            <thead>
                <tr>
                    <th>Shard ID</th>
                    <th>Documents</th>
                    <th>Distribution</th>
                    <th>Data Size (MB)</th>
                    <th>Avg Object Size (KB)</th>
                    <th>Chunks</th>
                </tr>
            </thead>
            <tbody>
    `;

    sortedShards.forEach(shard => {
        const percentage = shard.percentage || 0;

        let barColor = '#13aa52';
        if (percentage > 40) barColor = '#f97316';
        else if (percentage < 25) barColor = '#fbbf24';

        tableHTML += `
            <tr>
                <td><strong>${shard.shard_id}</strong></td>
                <td>${shard.documents.toLocaleString()}</td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-fill"
                             style="width:${percentage}%;background:${barColor}">
                            ${percentage.toFixed(2)}%
                        </div>
                    </div>
                </td>
                <td>${shard.data_size_mb.toFixed(2)}</td>
                <td>${shard.avg_obj_size_kb.toFixed(2)}</td>
                <td>${shard.chunks}</td>
            </tr>
        `;
    });

    tableHTML += `</tbody></table>`;

    // Borough totals (already correct)
    if (Object.keys(boroughTotals).length) {
        const total = Object.values(boroughTotals).reduce((a, b) => a + b, 0);

        tableHTML += `
            <h4 style="margin-top:30px;">Top Boroughs</h4>
            <table>
                <thead>
                    <tr>
                        <th>Borough</th>
                        <th>Trips</th>
                        <th>%</th>
                    </tr>
                </thead>
                <tbody>
        `;

        Object.entries(boroughTotals)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10)
            .forEach(([borough, count]) => {
                const pct = total ? ((count / total) * 100).toFixed(2) : 0;
                tableHTML += `
                    <tr>
                        <td>${borough}</td>
                        <td>${count.toLocaleString()}</td>
                        <td>${pct}%</td>
                    </tr>
                `;
            });

        tableHTML += `</tbody></table>`;
    }

    tableDiv.innerHTML = tableHTML;
}


/**
 * Create visualizations (charts)
 */
function createShardDistributionCharts(data) {
    const shards = data.shards || [];
    if (!shards.length) return;

    const labels = shards.map(s => s.shard_id);
    const documents = shards.map(s => s.documents);
    const sizesMB = shards.map(s => s.data_size_mb);

    const colors = generateShardColors(shards.length);

    createDocumentDistributionChart(labels, documents, colors, data);
    createDataSizeChart(labels, sizesMB);
}


/**
 * Create doughnut chart for document distribution
 */
function createDocumentDistributionChart(labels, data, colors, fullData) {
    const ctx = document.getElementById('shardDistributionChart');
    if (!ctx) {
        console.warn('Canvas element shardDistributionChart not found');
        return;
    }
    
    // Destroy existing chart
    if (shardDistributionChart) {
        shardDistributionChart.destroy();
    }
    
    shardDistributionChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                label: 'Documents per Shard',
                data: data,
                backgroundColor: colors,
                borderColor: '#fff',
                borderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Document Distribution Across Shards',
                    font: {
                        size: 16,
                        weight: 'bold'
                    },
                    padding: 20
                },
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 15,
                        font: {
                            size: 12
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
    const shard = fullData.shards[context.dataIndex];
    return [
        `Shard: ${shard.shard_id}`,
        `Documents: ${shard.documents.toLocaleString()}`,
        `Percentage: ${shard.percentage.toFixed(2)}%`
    ];
}
                    },
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: {
                        size: 14
                    },
                    bodyFont: {
                        size: 13
                    }
                }
            }
        }
    });
}

/**
 * Create bar chart for data size distribution
 */
function createDataSizeChart(labels, dataSizes) {
    const ctx = document.getElementById('boroughByShardChart');
    if (!ctx) {
        console.warn('Canvas element boroughByShardChart not found');
        return;
    }
    
    // Destroy existing chart
    if (boroughByShardChart) {
        boroughByShardChart.destroy();
    }
    
    boroughByShardChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Data Size (MB)',
                data: dataSizes,
                backgroundColor: 'rgba(102, 126, 234, 0.7)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 2,
                borderRadius: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Data Size Distribution Across Shards',
                    font: {
                        size: 16,
                        weight: 'bold'
                    },
                    padding: 20
                },
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    callbacks: {
                        label: function(context) {
                            return `Size: ${context.parsed.y} MB`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Size (MB)',
                        font: {
                            size: 13,
                            weight: 'bold'
                        }
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toFixed(1) + ' MB';
                        }
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Shard',
                        font: {
                            size: 13,
                            weight: 'bold'
                        }
                    },
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

/**
 * Generate colors for shards
 */
function generateShardColors(count) {
    const baseColors = [
        'rgba(102, 126, 234, 0.8)',  // Purple
        'rgba(19, 170, 82, 0.8)',     // Green
        'rgba(18, 135, 168, 0.8)',    // Blue
        'rgba(255, 159, 64, 0.8)',    // Orange
        'rgba(255, 99, 132, 0.8)',    // Red
        'rgba(153, 102, 255, 0.8)',   // Violet
        'rgba(75, 192, 192, 0.8)',    // Teal
        'rgba(255, 205, 86, 0.8)'     // Yellow
    ];
    
    const colors = [];
    for (let i = 0; i < count; i++) {
        colors.push(baseColors[i % baseColors.length]);
    }
    return colors;
}

// ============================================================================
// Optional: Auto-refresh functionality
// Uncomment to enable automatic updates every 30 seconds
// ============================================================================

/*
let shardDistributionAutoRefresh = null;

function startShardDistributionAutoRefresh() {
    stopShardDistributionAutoRefresh();
    shardDistributionAutoRefresh = setInterval(() => {
        // Only refresh if the MongoDB panel is active
        const mongoPanel = document.getElementById('mongoPanel');
        if (mongoPanel && mongoPanel.classList.contains('active')) {
            console.log('Auto-refreshing shard distribution...');
            analyzeShardDistribution();
        }
    }, 30000); // 30 seconds
}

function stopShardDistributionAutoRefresh() {
    if (shardDistributionAutoRefresh) {
        clearInterval(shardDistributionAutoRefresh);
        shardDistributionAutoRefresh = null;
    }
}

// Start auto-refresh when the page loads
window.addEventListener('load', startShardDistributionAutoRefresh);
*/

console.log('✅ Shard Distribution Visualization loaded successfully');