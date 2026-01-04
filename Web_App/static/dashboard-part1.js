// dashboard-part1.js - Core Setup and Socket Handlers
const socket = io('http://localhost:5000');

// Chart instances
let charts = {
    mongoRevenue: null,
    mongoTrips: null,
    mongoPassengers: null,
    mongoHourly: null,
    cassandraRevenue: null,
    cassandraHourly: null,
    cassandraPayment: null,
    comparison: null,
    throughput: null,
    revenueOverTime: null,
    throughputComparison: null
};

// Performance data
let performanceData = {
    mongo: [],
    cassandra: []
};

// Streaming metrics - SINGLE DECLARATION
let streamingMetrics = {
    timestamps: [],
    throughput: [],
    revenue: [],
    mongoInserts: 0,
    cassandraInserts: 0,
    lastMongoUpdate: Date.now(),
    lastCassandraUpdate: Date.now(),
    maxDataPoints: 20
};

// ============================================================================
// UTILITY FUNCTIONS - DEFINED FIRST
// ============================================================================

function capitalize(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function getAnomalyType(trip) {
    if (!trip) return 'Unknown';
    if (trip.trip_distance === 0) return 'Zero distance';
    if (trip.duration_minutes && trip.duration_minutes <= 0) return 'Invalid duration';
    if (trip.total_amount && trip.total_amount <= 0) return 'Invalid amount';
    return 'High speed';
}

function destroyChart(chartName) {
    if (charts[chartName]) {
        charts[chartName].destroy();
        charts[chartName] = null;
    }
}

function getChartOptions(yAxisLabel) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            y: {
                beginAtZero: true,
                title: { display: true, text: yAxisLabel }
            }
        },
        plugins: {
            legend: {
                display: true,
                position: 'top'
            }
        }
    };
}

// ============================================================================
// CHART UPDATE FUNCTIONS
// ============================================================================

function updateThroughputChart() {
    const ctx = document.getElementById('throughputChart');
    if (!ctx) {
        console.log('throughputChart canvas not found');
        return;
    }
    
    destroyChart('throughput');
    
    const labels = streamingMetrics.timestamps.length > 0 ? streamingMetrics.timestamps : ['Now'];
    const data = streamingMetrics.throughput.length > 0 ? streamingMetrics.throughput : [0];
    
    console.log('📊 Updating throughput chart with data:', { labels: labels.length, data: data.length, lastValue: data[data.length-1] });
    
    charts.throughput = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Messages/min',
                data: data,
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.2)',
                tension: 0.4,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Messages/minute' }
                }
            },
            plugins: {
                legend: { display: true, position: 'top' }
            },
            animation: {
                duration: 300
            }
        }
    });
}

function updateRevenueOverTimeChart() {
    const ctx = document.getElementById('revenueOverTimeChart');
    if (!ctx) {
        console.log('revenueOverTimeChart canvas not found');
        return;
    }
    
    destroyChart('revenueOverTime');
    
    const labels = streamingMetrics.timestamps.length > 0 ? streamingMetrics.timestamps : ['Now'];
    const data = streamingMetrics.revenue.length > 0 ? streamingMetrics.revenue : [0];
    
    console.log('📊 Updating revenue chart with data:', { labels: labels.length, data: data.length, lastValue: data[data.length-1] });
    
    charts.revenueOverTime = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Total Revenue ($)',
                data: data,
                borderColor: '#13aa52',
                backgroundColor: 'rgba(19, 170, 82, 0.2)',
                tension: 0.4,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Revenue ($)' },
                    ticks: {
                        callback: function(value) {
                            return '$' + value.toLocaleString();
                        }
                    }
                }
            },
            plugins: {
                legend: { display: true, position: 'top' }
            },
            animation: {
                duration: 300
            }
        }
    });
}

function updateRealTimeCharts() {
    console.log('🔄 updateRealTimeCharts called, data points:', streamingMetrics.timestamps.length);
    
    if (streamingMetrics.timestamps.length > 0) {
        updateThroughputChart();
        updateRevenueOverTimeChart();
    } else {
        console.log('⚠️ No data points yet to update charts');
    }
}

// ============================================================================
// UPDATE FUNCTIONS
// ============================================================================

function updateConnectionStatus(connected) {
    const statusEl = document.getElementById('streamingStatus');
    const indicatorEl = document.getElementById('statusIndicator');
    
    if (statusEl && indicatorEl) {
        if (connected) {
            statusEl.textContent = 'Connected';
            indicatorEl.className = 'status-indicator active';
        } else {
            statusEl.textContent = 'Disconnected';
            indicatorEl.className = 'status-indicator inactive';
        }
    }
}

function updateKafkaStatus(data) {
    const statusEl = document.getElementById('streamingStatus');
    const indicatorEl = document.getElementById('statusIndicator');
    
    if (statusEl && indicatorEl) {
        if (data.status === 'connected') {
            statusEl.textContent = 'Streaming Active';
            indicatorEl.className = 'status-indicator active';
        } else {
            statusEl.textContent = data.status;
            indicatorEl.className = 'status-indicator inactive';
        }
    }
}

function updateStreamingStats(stats) {
    if (!stats) {
        console.log('⚠️ updateStreamingStats called with no stats');
        return;
    }
    
    console.log('📊 updateStreamingStats called:', stats);
    
    // Update all counters
    const mongoEl = document.getElementById('mongoInserts');
    const cassandraEl = document.getElementById('cassandraInserts');
    const tripsEl = document.getElementById('tripsPerMinute');
    const revenueEl = document.getElementById('totalRevenue');
    
    if (mongoEl) mongoEl.textContent = (stats.mongo_inserts || stats.total_trips || 0).toLocaleString();
    if (cassandraEl) cassandraEl.textContent = (stats.cassandra_inserts || stats.total_trips || 0).toLocaleString();
    if (tripsEl) tripsEl.textContent = (stats.trips_per_minute || 0).toLocaleString();
    if (revenueEl) revenueEl.textContent = `$${(stats.total_revenue || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    
    // Update streaming metrics
    streamingMetrics.mongoInserts = stats.mongo_inserts || stats.total_trips || 0;
    streamingMetrics.cassandraInserts = stats.cassandra_inserts || stats.total_trips || 0;
    
    // Calculate and display rates
    const now = Date.now();
    const timeSinceLastMongo = (now - streamingMetrics.lastMongoUpdate) / 1000;
    const timeSinceLastCassandra = (now - streamingMetrics.lastCassandraUpdate) / 1000;
    
    const mongoRate = document.getElementById('mongoRate');
    const cassandraRate = document.getElementById('cassandraRate');
    
    if (mongoRate && timeSinceLastMongo > 0.5) {
        const rate = Math.round((stats.trips_per_minute || 0) / 60);
        mongoRate.textContent = `${rate} /sec`;
        streamingMetrics.lastMongoUpdate = now;
    }
    
    if (cassandraRate && timeSinceLastCassandra > 0.5) {
        const rate = Math.round((stats.trips_per_minute || 0) / 60);
        cassandraRate.textContent = `${rate} /sec`;
        streamingMetrics.lastCassandraUpdate = now;
    }
    
    // Update metrics for charts - THIS IS CRITICAL
    const timestamp = new Date().toLocaleTimeString('en-US', { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit',
        hour12: false 
    });
    
    streamingMetrics.timestamps.push(timestamp);
    streamingMetrics.throughput.push(stats.trips_per_minute || 0);
    streamingMetrics.revenue.push(stats.total_revenue || 0);
    
    console.log('📈 Added data point:', {
        timestamp,
        throughput: stats.trips_per_minute || 0,
        revenue: stats.total_revenue || 0,
        totalPoints: streamingMetrics.timestamps.length
    });
    
    // Keep only last N data points
    if (streamingMetrics.timestamps.length > streamingMetrics.maxDataPoints) {
        streamingMetrics.timestamps.shift();
        streamingMetrics.throughput.shift();
        streamingMetrics.revenue.shift();
        console.log('🗑️ Removed old data point, keeping last', streamingMetrics.maxDataPoints);
    }
    
    // Update recent trips
    if (stats.recent_trips && Array.isArray(stats.recent_trips)) {
        updateRecentTrips(stats.recent_trips);
    }
    
    // Update status displays
    updateSystemStatus();
    
    // FORCE UPDATE CHARTS IMMEDIATELY
    updateRealTimeCharts();
}

function updateRecentTrips(trips) {
    const listEl = document.getElementById('recentTripsList');
    if (!listEl) return;
    
    listEl.innerHTML = trips.slice(0, 10).map(trip => `
        <div class="trip-item">
            <div>
                <strong>${trip.pickup_zone || 'Unknown'}</strong> → ${trip.dropoff_zone || 'Unknown'}
                <br>
                <small style="color: #666;">${trip.timestamp || ''} | ${trip.borough || 'Unknown'}</small>
            </div>
            <div style="text-align: right;">
                <strong>$${(trip.amount || 0).toFixed(2)}</strong>
                <br>
                <small style="color: #666;">${(trip.distance || 0).toFixed(2)} mi | ${trip.passengers || 0} pax</small>
            </div>
        </div>
    `).join('');
}

function updateSystemStatus() {
    // Update MongoDB status
    const mongoStatus = document.getElementById('mongoStatus');
    if (mongoStatus) {
        const isActive = streamingMetrics.mongoInserts > 0;
        const rate = streamingMetrics.throughput.length > 0 
            ? Math.round(streamingMetrics.throughput[streamingMetrics.throughput.length - 1] / 60) 
            : 0;
        
        mongoStatus.innerHTML = `
            <p><strong>Status:</strong> <span style="color: ${isActive ? '#13aa52' : '#999'};">● ${isActive ? 'Active' : 'Waiting...'}</span></p>
            <p><strong>Total Inserts:</strong> ${streamingMetrics.mongoInserts.toLocaleString()}</p>
            <p><strong>Insert Rate:</strong> ${rate}/sec</p>
            <p><strong>Collection:</strong> taxi_trips</p>
        `;
    }
    
    // Update Cassandra status
    const cassandraStatus = document.getElementById('cassandraStatus');
    if (cassandraStatus) {
        const isActive = streamingMetrics.cassandraInserts > 0;
        const rate = streamingMetrics.throughput.length > 0 
            ? Math.round(streamingMetrics.throughput[streamingMetrics.throughput.length - 1] / 60) 
            : 0;
        
        cassandraStatus.innerHTML = `
            <p><strong>Status:</strong> <span style="color: ${isActive ? '#1287a8' : '#999'};">● ${isActive ? 'Active' : 'Waiting...'}</span></p>
            <p><strong>Total Inserts:</strong> ${streamingMetrics.cassandraInserts.toLocaleString()}</p>
            <p><strong>Insert Rate:</strong> ${rate}/sec</p>
            <p><strong>Keyspace:</strong> Projet_bd_Rf3</p>
        `;
    }
}

// ============================================================================
// SOCKET.IO EVENT HANDLERS
// ============================================================================

socket.on('connect', () => {
    console.log('✅ Connected to server');
    updateConnectionStatus(true);
    // Request initial stats immediately
    socket.emit('request_stats');
});

socket.on('disconnect', () => {
    console.log('❌ Disconnected from server');
    updateConnectionStatus(false);
});

socket.on('connection_response', (data) => {
    console.log('🔌 Connection response received:', data);
    if (data.stats) {
        console.log('📊 Initial stats from connection_response:', data.stats);
        updateStreamingStats(data.stats);
    }
    if (data.kafka_status) {
        updateKafkaStatus({ status: data.kafka_status });
    }
});

socket.on('kafka_status', (data) => {
    console.log('📡 Kafka status update:', data);
    updateKafkaStatus(data);
});

socket.on('new_trip', (data) => {
    console.log('🚕 New trip received:', data);
    if (data.stats) {
        updateStreamingStats(data.stats);
    }
});

// Periodic stats request - request fresh data every 5 seconds
setInterval(() => {
    if (socket.connected) {
        console.log('🔄 Requesting stats update...');
        socket.emit('request_stats');
    }
}, 5000);

// ============================================================================
// TAB NAVIGATION
// ============================================================================

function showTab(tabName) {
    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
    document.querySelectorAll('.content-panel').forEach(panel => panel.classList.remove('active'));
    
    event.target.classList.add('active');
    const panel = document.getElementById(tabName + 'Panel');
    if (panel) {
        panel.classList.add('active');
        
        // If switching to overview panel, refresh charts
        if (tabName === 'overview' && streamingMetrics.timestamps.length > 0) {
            console.log('🔄 Switched to overview, refreshing charts');
            setTimeout(() => {
                updateRealTimeCharts();
            }, 100);
        }
    }
}

// ============================================================================
// INITIALIZATION
// ============================================================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('📊 Dashboard loading...');
    
    // Initialize empty charts
    console.log('📊 Initializing charts...');
    updateThroughputChart();
    updateRevenueOverTimeChart();
    
    // Initialize status displays
    updateSystemStatus();
    
    // Request initial stats
    console.log('📡 Requesting initial stats...');
    socket.emit('request_stats');
    
    // Refresh system status display every 2 seconds
    setInterval(() => {
        updateSystemStatus();
    }, 2000);
    
    console.log('✅ Dashboard Part 1 initialized successfully');
    console.log('📊 Current streaming metrics:', streamingMetrics);
});

// Debug function
window.debugDashboard = function() {
    console.log('=== Dashboard Debug Info ===');
    console.log('Socket connected:', socket.connected);
    console.log('Streaming metrics:', streamingMetrics);
    console.log('Charts:', Object.keys(charts).filter(k => charts[k] !== null));
    console.log('Performance data:', performanceData);
    console.log('Timestamps:', streamingMetrics.timestamps);
    console.log('Throughput data:', streamingMetrics.throughput);
    console.log('Revenue data:', streamingMetrics.revenue);
    console.log('===========================');
};

// Add test data function for debugging
window.addTestData = function() {
    console.log('🧪 Adding test data...');
    const testStats = {
        mongo_inserts: Math.floor(Math.random() * 1000),
        cassandra_inserts: Math.floor(Math.random() * 1000),
        trips_per_minute: Math.floor(Math.random() * 100),
        total_revenue: Math.random() * 10000,
        recent_trips: []
    };
    updateStreamingStats(testStats);
    console.log('✅ Test data added');
};
fetch('/api/latency')
  .then(res => res.json())
  .then(data => {
    latencyChart.data.labels = data.latencies.map((_, i) => i);
    latencyChart.data.datasets[0].data = data.latencies;
    latencyChart.update();
  });
let latencyChart = new Chart(
    document.getElementById('mongoLatencyChart'),
    {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Latency (ms)',
                data: [],
                borderWidth: 2,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Message Index'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Latency (ms)'
                    }
                }
            }
        }
    }
);
