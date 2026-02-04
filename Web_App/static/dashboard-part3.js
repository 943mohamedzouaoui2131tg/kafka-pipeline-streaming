// dashboard-part3.js - Cassandra Charts, Comparisons and Scalability

// API Base URLs
const MONGO_URL = 'http://localhost:5000/mongo';
const CASSANDRA_URL = 'http://localhost:5000/cassandra';

// Global variable for scalability chart
let scalabilityChart = null;

// ============================================================================
// CASSANDRA CHARTS
// ============================================================================

function renderCassandraRevenueChart(data) {
    destroyChart('cassandraRevenue');
    const ctx = document.getElementById('cassandraRevenueChart');
    if (!ctx || !data || data.length === 0) return;
    
    const topData = data.slice(0, 10);
    charts.cassandraRevenue = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: topData.map(d => d.pickup_zone || 'Unknown'),
            datasets: [{
                label: 'Total Revenue ($)',
                data: topData.map(d => d.total_revenue || 0),
                backgroundColor: 'rgba(18, 135, 168, 0.8)',
                borderColor: 'rgba(18, 135, 168, 1)',
                borderWidth: 2
            }]
        },
        options: getChartOptions('Revenue ($)')
    });
}

function renderCassandraHourlyChart(data) {
    destroyChart('cassandraHourly');
    const ctx = document.getElementById('cassandraHourlyChart');
    if (!ctx || !data || data.length === 0) return;
    
    charts.cassandraHourly = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
            labels: data.map(d => `${d.pickup_hour}:00`),
            datasets: [{
                label: 'Revenue ($)',
                data: data.map(d => d.total_revenue || 0),
                borderColor: '#1287a8',
                backgroundColor: 'rgba(18, 135, 168, 0.2)',
                tension: 0.4,
                fill: true
            }]
        },
        options: getChartOptions('Revenue ($)')
    });
}

function renderCassandraPaymentChart(data) {
    destroyChart('cassandraPayment');
    const ctx = document.getElementById('cassandraPaymentChart');
    if (!ctx || !data || data.length === 0) return;
    
    charts.cassandraPayment = new Chart(ctx.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: data.map(d => capitalize(d.payment_type)),
            datasets: [{
                data: data.map(d => d.count || 0),
                backgroundColor: [
                    '#667eea', '#764ba2', '#f093fb', '#4facfe',
                    '#43e97b', '#fa709a'
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

// ============================================================================
// ANOMALY VISUALIZATION
// ============================================================================

function renderAnomaliesVisualization(data) {
    const container = document.getElementById('anomaliesVisualization');
    if (!container) return;
    
    const anomalyTypes = {
        'Zero distance': 0,
        'Invalid duration': 0,
        'Invalid amount': 0,
        'High speed': 0
    };
    
    data.forEach(trip => {
        const type = getAnomalyType(trip);
        anomalyTypes[type]++;
    });
    
    const html = `
        <div class="chart-grid">
            <div style="min-height: 400px;">
                <h3>Anomaly Types Distribution</h3>
                <div class="chart-container">
                    <canvas id="anomalyTypeChart"></canvas>
                </div>
            </div>
            <div>
                <h3>Detected Anomalies Summary</h3>
                ${Object.entries(anomalyTypes).map(([type, count]) => `
                    <div class="anomaly-card ${count > 10 ? 'critical' : ''}">
                        <strong>${type}:</strong> ${count} trips
                    </div>
                `).join('')}
            </div>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Pickup Zone</th>
                    <th>Distance</th>
                    <th>Duration</th>
                    <th>Amount</th>
                    <th>Issue</th>
                </tr>
            </thead>
            <tbody>
                ${data.slice(0, 20).map(trip => `
                    <tr>
                        <td>${trip.PULocationID?.zone || 'Unknown'}</td>
                        <td>${(trip.trip_distance || 0).toFixed(2)} mi</td>
                        <td>${trip.duration_minutes ? trip.duration_minutes.toFixed(2) : 'N/A'} min</td>
                        <td>$${(trip.total_amount || 0).toFixed(2)}</td>
                        <td><span class="anomaly-card">${getAnomalyType(trip)}</span></td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
    
    container.innerHTML = html;
    
    // Render pie chart
    setTimeout(() => {
        const ctx = document.getElementById('anomalyTypeChart');
        if (ctx) {
            new Chart(ctx.getContext('2d'), {
                type: 'pie',
                data: {
                    labels: Object.keys(anomalyTypes),
                    datasets: [{
                        data: Object.values(anomalyTypes),
                        backgroundColor: ['#ffc107', '#dc3545', '#fd7e14', '#6f42c1']
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false
                }
            });
        }
    }, 100);
}

// ============================================================================
// COMPARISON CHARTS
// ============================================================================

function updateComparisonChart() {
    const ctx = document.getElementById('comparisonChart');
    if (!ctx) return;
    
    destroyChart('comparison');
    
    const queries = [...new Set([
        ...performanceData.mongo.map(d => d.query),
        ...performanceData.cassandra.map(d => d.query)
    ])];

    if (queries.length === 0) return;

    charts.comparison = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: queries.map(q => capitalize(q)),
            datasets: [
                {
                    label: 'MongoDB (ms)',
                    data: queries.map(q => {
                        const match = performanceData.mongo.find(d => d.query === q);
                        return match ? match.time : 0;
                    }),
                    backgroundColor: 'rgba(19, 170, 82, 0.8)'
                },
                {
                    label: 'Cassandra (ms)',
                    data: queries.map(q => {
                        const match = performanceData.cassandra.find(d => d.query === q);
                        return match ? match.time : 0;
                    }),
                    backgroundColor: 'rgba(18, 135, 168, 0.8)'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Execution Time (ms)' }
                }
            }
        }
    });
    
    updateComparisonStats();
}

function updateThroughputComparisonChart() {
    const ctx = document.getElementById('throughputComparisonChart');
    if (!ctx) return;
    
    destroyChart('throughputComparison');
    
    const mongoRate = streamingMetrics.throughput.length > 0 
        ? Math.round(streamingMetrics.throughput[streamingMetrics.throughput.length - 1] / 60)
        : 0;
    
    charts.throughputComparison = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: ['MongoDB', 'Cassandra'],
            datasets: [{
                label: 'Inserts/second',
                data: [mongoRate, mongoRate],
                backgroundColor: ['rgba(19, 170, 82, 0.8)', 'rgba(18, 135, 168, 0.8)']
            }]
        },
        options: getChartOptions('Inserts/second')
    });
}

function updateComparisonStats() {
    const mongoStats = document.getElementById('mongoComparisonStats');
    const cassandraStats = document.getElementById('cassandraComparisonStats');
    
    if (mongoStats && performanceData.mongo.length > 0) {
        const avgTime = performanceData.mongo.reduce((sum, d) => sum + d.time, 0) / performanceData.mongo.length;
        mongoStats.innerHTML = `
            <p><strong>Queries Executed:</strong> ${performanceData.mongo.length}</p>
            <p><strong>Average Time:</strong> ${avgTime.toFixed(2)}ms</p>
        `;
    }
    
    if (cassandraStats && performanceData.cassandra.length > 0) {
        const avgTime = performanceData.cassandra.reduce((sum, d) => sum + d.time, 0) / performanceData.cassandra.length;
        cassandraStats.innerHTML = `
            <p><strong>Queries Executed:</strong> ${performanceData.cassandra.length}</p>
            <p><strong>Average Time:</strong> ${avgTime.toFixed(2)}ms</p>
        `;
    }
}

// ============================================================================
// CLUSTER INFORMATION
// ============================================================================

async function getMongoShardInfo() {
    const container = document.getElementById('shardCards');
    if (!container) return;
    
    container.innerHTML = '<div class="loading">Loading shard information...</div>';
    
    try {
        const response = await fetch('/mongo/cluster/shard-status');
        const result = await response.json();
        
        if (!result.success || result.error) {
            throw new Error(result.error || 'Failed to fetch shard information');
        }
        
        const shards = result.shards || [];
        const totalDocs = result.totalDocuments || 0;
        const totalSize = result.totalDataSize || 0;
        
        let html = `
            <div class="shard-card" style="grid-column: 1 / -1; border-color: #667eea;">
                <h4>📊 Cluster Summary</h4>
                <p><strong>Database:</strong> ${result.database}</p>
                <p><strong>Collection:</strong> ${result.collection}</p>
                <p><strong>Total Shards:</strong> ${result.totalShards}</p>
                <p><strong>Total Documents:</strong> ${totalDocs.toLocaleString()}</p>
                <p><strong>Total Data Size:</strong> ${formatBytes(totalSize)}</p>
                <p><strong>Sharded:</strong> ${result.sharded ? '✅ Yes' : '❌ No'}</p>
            </div>
        `;
        
        html += shards.map(shard => {
            const docPercentage = totalDocs > 0 ? (shard.documents / totalDocs * 100) : 0;
            const sizePercentage = totalSize > 0 ? (shard.dataSize / totalSize * 100) : 0;
            const isConnected = shard.status === 'connected';
            
            return `
                <div class="shard-card" style="border-color: ${isConnected ? '#13aa52' : '#dc3545'};">
                    <h4>
                        ${shard.name}
                        <span class="status-indicator ${isConnected ? 'active' : 'inactive'}"></span>
                    </h4>
                    <p><strong>Host:</strong> ${shard.host}</p>
                    <p><strong>Status:</strong> ${isConnected ? '✅ Connected' : '❌ Disconnected'}</p>
                    <p><strong>Documents:</strong> ${shard.documents.toLocaleString()}</p>
                    <p><strong>Data Size:</strong> ${formatBytes(shard.dataSize)}</p>
                    <p><strong>Storage Size:</strong> ${formatBytes(shard.storageSize)}</p>
                    ${shard.chunks !== undefined ? `<p><strong>Chunks:</strong> ${shard.chunks}</p>` : ''}
                    
                    <div style="margin-top: 10px;">
                        <small>Document Distribution:</small>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: ${docPercentage.toFixed(1)}%"></div>
                        </div>
                        <small>${docPercentage.toFixed(1)}% of total documents</small>
                    </div>
                </div>
            `;
        }).join('');
        
        container.innerHTML = html;
        
    } catch (error) {
        console.error('Error fetching MongoDB shard info:', error);
        container.innerHTML = `<div style="color: red; padding: 20px;">Error: ${error.message}</div>`;
    }
}

async function getCassandraClusterInfo() {
    const container = document.getElementById('cassandraClusterInfo');
    if (!container) return;
    
    container.innerHTML = '<div class="loading">Loading cluster information...</div>';
    
    try {
        const response = await fetch('/cassandra/cluster/status');
        const result = await response.json();
        
        if (!result.success || result.error) {
            throw new Error(result.error || 'Failed to fetch cluster information');
        }
        
        const cluster = result.cluster || {};
        const keyspaces = result.keyspaces || [];
        const nodes = cluster.nodes || [];
        
        let html = `
            <div class="db-card" style="margin-bottom: 20px;">
                <h4>📊 Cluster Overview</h4>
                <p><strong>Cluster Name:</strong> ${cluster.name || 'N/A'}</p>
                <p><strong>Total Nodes:</strong> ${cluster.nodeCount || 0}</p>
                <p><strong>Total Keyspaces:</strong> ${result.totalKeyspaces || 0}</p>
                <p><strong>Total Tables:</strong> ${result.totalTables || 0}</p>
                <p><strong>Total Rows:</strong> ${(result.totalRows || 0).toLocaleString()}</p>
            </div>
        `;
        
        if (nodes.length > 0) {
            html += `
                <div class="db-card" style="margin-bottom: 20px;">
                    <h4>🖥️ Nodes Status</h4>
                    <div class="shard-info">
                        ${nodes.map(node => `
                            <div class="shard-card" style="border-color: #1813aa">
                                <h5>Node: ${node.address}</h5>
                                <p><strong>Datacenter:</strong> ${node.datacenter}</p>
                                <p><strong>Rack:</strong> ${node.rack}</p>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }
        
        html += `<h4 style="margin-top: 20px;">📚 Keyspaces & Replication Factors</h4>`;
        
        keyspaces.forEach(ks => {
            const tables = ks.tables || [];
            html += `
                <div class="db-card" style="margin-bottom: 20px; border-left: 4px solid #1287a8;">
                    <h4>${ks.name}</h4>
                    <p><strong>Replication Factor:</strong> ${ks.replicationFactor}</p>
                    <p><strong>Replication Strategy:</strong> ${ks.replicationStrategy}</p>
                    <p><strong>Tables:</strong> ${ks.tableCount}</p>
                    <p><strong>Total Rows:</strong> ${ks.totalRows.toLocaleString()}</p>
                    
                    <div style="margin-top: 15px;">
                        <h5>Tables:</h5>
                        <table style="margin-top: 10px;">
                            <thead>
                                <tr>
                                    <th>Table Name</th>
                                    <th>Rows</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${tables.map(table => `
                                    <tr>
                                        <td>${table.name}</td>
                                        <td>${(table.rows || 0).toLocaleString()}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        });
        
        container.innerHTML = html;
        
    } catch (error) {
        console.error('Error fetching Cassandra cluster info:', error);
        container.innerHTML = `<div style="color: red; padding: 20px;">Error: ${error.message}</div>`;
    }
}

// ============================================================================
// SCALABILITY TESTING
// ============================================================================

// Show/hide load level control based on test type
document.getElementById('scalabilityTestType')?.addEventListener('change', function() {
    const loadLevelGroup = document.getElementById('loadLevelGroup');
    if (this.value === 'load') {
        loadLevelGroup.style.display = 'block';
    } else {
        loadLevelGroup.style.display = 'none';
    }
});

async function runScalabilityTest() {
    const testType = document.getElementById('scalabilityTestType').value;
    const database = document.getElementById('scalabilityDatabase').value;
    const resultsDiv = document.getElementById('scalabilityResults');
    
    resultsDiv.innerHTML = '<div class="loading">🔄 Running scalability tests...</div>';
    
    try {
        let results = {};
        
        if (database === 'both' || database === 'mongo') {
            try {
                results.mongo = await runDatabaseTest('mongo', testType);
            } catch (error) {
                console.error('MongoDB test failed:', error);
                results.mongo = { error: error.message, database: 'mongo' };
            }
        }
        
        if (database === 'both' || database === 'cassandra') {
            try {
                results.cassandra = await runDatabaseTest('cassandra', testType);
            } catch (error) {
                console.error('Cassandra test failed:', error);
                results.cassandra = { error: error.message, database: 'cassandra' };
            }
        }
        
        const hasResults = Object.values(results).some(r => !r.error);
        
        if (!hasResults) {
            resultsDiv.innerHTML = `
                <div class="error">
                    ❌ All tests failed. Please check:
                    <ul>
                        <li>Flask server is running on http://localhost:5000</li>
                        <li>Endpoints are accessible</li>
                        <li>Check browser console (F12) for details</li>
                    </ul>
                </div>
            `;
            return;
        }
        
        displayScalabilityResults(results, testType);
        
    } catch (error) {
        resultsDiv.innerHTML = `<div class="error">❌ Error: ${error.message}</div>`;
        console.error('Scalability test error:', error);
    }
}

async function runDatabaseTest(dbType, testType) {
    const baseUrl = dbType === 'mongo' ? MONGO_URL : CASSANDRA_URL;
    
    console.log(`🧪 Testing ${dbType} - ${testType}`);
    
    switch (testType) {
        case 'nodes':
            return await runNodeScalingTest(baseUrl, dbType);
        case 'load':
            return await runLoadTest(baseUrl, dbType);
        case 'batch':
            return await runBatchStreamingTest(baseUrl, dbType);
        default:
            throw new Error('Unknown test type');
    }
}

async function runNodeScalingTest(baseUrl, dbType) {
    const testTypes = ['read', 'write', 'aggregate'];
    const results = [];
    
    for (const type of testTypes) {
        try {
            const url = `${baseUrl}/scalability/node-scaling?test_type=${type}`;
            console.log(`Fetching: ${url}`);
            
            const response = await fetch(url);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                const text = await response.text();
                console.error('Non-JSON response:', text.substring(0, 200));
                throw new Error('Server returned HTML instead of JSON');
            }
            
            const data = await response.json();
            console.log(`${dbType} ${type} test result:`, data);
            
            if (data.success) {
                results.push({
                    testType: type,
                    ...data.data,
                    hosts: data.hosts || data.shards || []
                });
            }
        } catch (error) {
            console.error(`Error testing ${type}:`, error);
            results.push({
                testType: type,
                error: error.message,
                execution_time_ms: 0,
                throughput: 0,
                cpu_percent: 0,
                memory_percent: 0
            });
        }
    }
    
    if (results.length === 0) {
        throw new Error(`No tests completed for ${dbType}`);
    }
    
    return {
        testName: 'Node Scaling',
        database: dbType,
        results: results
    };
}

async function runLoadTest(baseUrl, dbType) {
    const loadLevel = document.getElementById('loadLevel').value;
    
    const url = `${baseUrl}/scalability/load-testing?load=${loadLevel}`;
    const response = await fetch(url);
    const data = await response.json();
    
    if (!data.success) {
        throw new Error(data.error || 'Load test failed');
    }
    
    return {
        testName: 'Load Testing',
        database: dbType,
        loadLevel: parseInt(loadLevel),
        results: [data.data]
    };
}

async function runBatchStreamingTest(baseUrl, dbType) {
    if (dbType !== 'mongo') {
        return {
            testName: 'Batch vs Streaming',
            database: dbType,
            results: [],
            note: 'This test is only available for MongoDB'
        };
    }
    
    const url = `${baseUrl}/scalability/batch-vs-streaming?records=1000`;
    const response = await fetch(url);
    const data = await response.json();
    
    if (!data.success) {
        throw new Error(data.error || 'Batch/streaming test failed');
    }
    
    return {
        testName: 'Batch vs Streaming',
        database: dbType,
        results: [data.data]
    };
}

function displayScalabilityResults(results, testType) {
    const resultsDiv = document.getElementById('scalabilityResults');
    let html = '<div class="results-container">';
    
    for (const [dbType, data] of Object.entries(results)) {
        const dbIcon = dbType === 'mongo' ? '🍃' : '💎';
        const dbName = dbType === 'mongo' ? 'MongoDB' : 'Cassandra';
        
        html += `<div class="db-results">`;
        html += `<h4>${dbIcon} ${dbName}</h4>`;
        
        if (data.error) {
            html += `<div class="error-message">❌ ${data.error}</div>`;
        } else {
            html += formatTestResults(data, testType);
        }
        
        html += `</div>`;
    }
    
    html += '</div>';
    resultsDiv.innerHTML = html;
    
    const hasValidResults = Object.values(results).some(r => !r.error && r.results && r.results.length > 0);
    if (hasValidResults) {
        updateScalabilityChart(results, testType);
    }
}

function formatTestResults(data, testType) {
    if (data.note) {
        return `<p class="note">ℹ️ ${data.note}</p>`;
    }
    
    let html = '<div class="metrics-grid">';
    
    data.results.forEach(result => {
        const hasError = result.error;
        const statusIcon = hasError ? '❌' : '✅';
        
        html += `
            <div class="metric-card ${hasError ? 'error-card' : ''}">
                <div class="metric-label">${statusIcon} ${result.testType ? result.testType.toUpperCase() : 'Test'}</div>
                ${hasError ? 
                    `<div class="metric-error">${result.error}</div>` :
                    `
                    <div class="metric-value">${result.execution_time_ms?.toFixed(2) || 0} ms</div>
                    <div class="metric-details">
                        <div>Throughput: ${result.throughput?.toFixed(2) || 0} ops/s</div>
                        <div>CPU: ${result.cpu_percent?.toFixed(1) || 0}%</div>
                        <div>Memory: ${result.memory_percent?.toFixed(1) || 0}%</div>
                    </div>
                    `
                }
            </div>
        `;
    });
    
    html += '</div>';
    return html;
}

function updateScalabilityChart(results, testType) {
    const ctx = document.getElementById('scalabilityChart');
    if (!ctx) return;
    
    if (scalabilityChart) {
        scalabilityChart.destroy();
    }
    
    const labels = ['Read', 'Write', 'Aggregate'];
    const datasets = [];
    
    for (const [dbType, data] of Object.entries(results)) {
        if (data.error || !data.results) continue;
        
        const color = dbType === 'mongo' ? 'rgba(76, 175, 80, 0.8)' : 'rgba(33, 150, 243, 0.8)';
        const execTimes = data.results.map(r => r.error ? 0 : r.execution_time_ms);
        
        datasets.push({
            label: dbType === 'mongo' ? 'MongoDB' : 'Cassandra',
            data: execTimes,
            backgroundColor: color,
            borderColor: color,
            borderWidth: 1
        });
    }
    
    scalabilityChart = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Time (ms)' }
                }
            }
        }
    });
}

console.log('✅ Dashboard Part 3 initialized successfully');