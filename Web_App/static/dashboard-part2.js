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
// OVERVIEW CHARTS
// ============================================================================

function updateThroughputChart() {
    const ctx = document.getElementById('throughputChart');
    if (!ctx) return;
    
    destroyChart('throughput');
    
    const labels = streamingMetrics.timestamps.length > 0 ? streamingMetrics.timestamps : ['Now'];
    const data = streamingMetrics.throughput.length > 0 ? streamingMetrics.throughput : [0];
    
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
            }
        }
    });
}

function updateRevenueOverTimeChart() {
    const ctx = document.getElementById('revenueOverTimeChart');
    if (!ctx) return;
    
    destroyChart('revenueOverTime');
    
    const labels = streamingMetrics.timestamps.length > 0 ? streamingMetrics.timestamps : ['Now'];
    const data = streamingMetrics.revenue.length > 0 ? streamingMetrics.revenue : [0];
    
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
            }
        }
    });
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