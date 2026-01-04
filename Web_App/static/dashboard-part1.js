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

// Streaming metrics
let streamingMetrics = {
    timestamps: [],
    throughput: [],
    revenue: [],
    mongoInserts: 0,
    cassandraInserts: 0,
    lastMongoUpdate: Date.now(),
    lastCassandraUpdate: Date.now()
};

// ============================================================================
// SOCKET.IO EVENT HANDLERS
// ============================================================================

socket.on('connect', () => {
    console.log('✅ Connected to server');
    updateConnectionStatus(true);
});

socket.on('disconnect', () => {
    console.log('❌ Disconnected from server');
    updateConnectionStatus(false);
});

socket.on('connection_response', (data) => {
    console.log('Connection response:', data);
    if (data.stats) {
        updateStreamingStats(data.stats);
    }
});

socket.on('kafka_status', (data) => {
    console.log('Kafka status:', data);
    updateKafkaStatus(data);
});

socket.on('new_trip', (data) => {
    updateStreamingStats(data.stats);
    updateRealTimeCharts();
});

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
    if (!stats) return;
    
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
    
    // Update metrics for charts
    const timestamp = new Date().toLocaleTimeString();
    streamingMetrics.timestamps.push(timestamp);
    streamingMetrics.throughput.push(stats.trips_per_minute || 0);
    streamingMetrics.revenue.push(stats.total_revenue || 0);
    
    // Keep only last 20 data points
    if (streamingMetrics.timestamps.length > 20) {
        streamingMetrics.timestamps.shift();
        streamingMetrics.throughput.shift();
        streamingMetrics.revenue.shift();
    }
    
    // Update recent trips
    if (stats.recent_trips && Array.isArray(stats.recent_trips)) {
        updateRecentTrips(stats.recent_trips);
    }
    
    // Update status displays
    updateSystemStatus();
}

function updateRecentTrips(trips) {
    const listEl = document.getElementById('recentTripsList');
    if (!listEl) return;
    
    listEl.innerHTML = trips.map(trip => `
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

function updateRealTimeCharts() {
    if (streamingMetrics.timestamps.length > 0) {
        updateThroughputChart();
        updateRevenueOverTimeChart();
    }
}

// ============================================================================
// TAB NAVIGATION
// ============================================================================

function showTab(tabName) {
    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
    document.querySelectorAll('.content-panel').forEach(panel => panel.classList.remove('active'));
    
    event.target.classList.add('active');
    const panel = document.getElementById(tabName + 'Panel');
    if (panel) panel.classList.add('active');
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

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