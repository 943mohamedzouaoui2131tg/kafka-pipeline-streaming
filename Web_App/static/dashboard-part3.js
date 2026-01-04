// dashboard-part3.js - Cassandra Charts, Comparisons and Initialization

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
// CLUSTER INFORMATION - ENHANCED
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
        
        // Display summary first
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
        
        // Display individual shards
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
                    
                    <div style="margin-top: 10px;">
                        <small>Size Distribution:</small>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: ${sizePercentage.toFixed(1)}%; background: linear-gradient(90deg, #13aa52 0%, #0f7c3d 100%);"></div>
                        </div>
                        <small>${sizePercentage.toFixed(1)}% of total size</small>
                    </div>
                    
                    ${shard.error ? `<p style="color: #dc3545; margin-top: 10px;"><small>Error: ${shard.error}</small></p>` : ''}
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
        
        // Display cluster overview
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
        
        // Display nodes information
        if (nodes.length > 0) {
            html += `
                <div class="db-card" style="margin-bottom: 20px;">
                    <h4>🖥️ Nodes Status</h4>
                    <div class="shard-info">
                        ${nodes.map(node => `
                            <div class="shard-card" style="border-color: ${node.is_up ? '#13aa52' : '#dc3545'};">
                                <h5>Node: ${node.address}</h5>
                                <p><strong>Status:</strong> 
                                    <span class="status-indicator ${node.is_up ? 'active' : 'inactive'}"></span>
                                    ${node.status === 'connected' || node.is_up ? '✅ Connected' : '❌ Disconnected'}
                                </p>
                                <p><strong>Datacenter:</strong> ${node.datacenter}</p>
                                <p><strong>Rack:</strong> ${node.rack}</p>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }
        
        // Display keyspaces with their tables
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
                    <p><strong>Total Replicas:</strong> ${ks.replicas}</p>
                    
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

// Keyspace details helper function
async function getCassandraKeyspaceDetails(keyspaceName) {
    try {
        const response = await fetch(`/cassandra/cluster/keyspace-details?keyspace=${keyspaceName}`);
        const result = await response.json();
        
        if (!result.success || result.error) {
            throw new Error(result.error || 'Failed to fetch keyspace details');
        }
        
        return result;
    } catch (error) {
        console.error('Error fetching keyspace details:', error);
        return null;
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function getAnomalyType(trip) {
    if (trip.trip_distance === 0) return 'Zero distance';
    if (trip.duration_minutes && trip.duration_minutes <= 0) return 'Invalid duration';
    if (trip.total_amount <= 0) return 'Invalid amount';
    if (trip.duration_minutes && trip.trip_distance) {
        const speed = trip.trip_distance / (trip.duration_minutes / 60);
        if (speed > 100) return 'High speed';
    }
    return 'Other';
}

function capitalize(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1);
}

// ============================================================================
// INITIALIZATION
// ============================================================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('📊 Dashboard loaded and initializing...');
    
    // Initialize empty charts once
    updateThroughputChart();
    updateRevenueOverTimeChart();
    
    // Initialize status displays
    updateSystemStatus();
    
    // Request initial stats from server
    socket.emit('request_stats');
    
    // Refresh system status every 2 seconds (but NOT charts - they update on new_trip event)
    setInterval(() => {
        updateSystemStatus();
    }, 2000);
    
    console.log('✅ Dashboard initialized successfully');
});

// Debug function
window.debugDashboard = function() {
    console.log('=== Dashboard Debug Info ===');
    console.log('Socket connected:', socket.connected);
    console.log('Streaming metrics:', streamingMetrics);
    console.log('Charts:', Object.keys(charts).filter(k => charts[k] !== null));
    console.log('Performance data:', performanceData);
    console.log('===========================');
};