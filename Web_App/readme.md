# 🚕 NYC Taxi Analytics Dashboard

A real-time analytics dashboard for NYC taxi data with Kafka streaming, MongoDB, and Cassandra integration.

## 📋 Features

- **Real-time Kafka Streaming**: Live trip data visualization with WebSocket updates
- **MongoDB Analytics**: Complex aggregation queries with execution time tracking
- **Cassandra Analytics**: Time-series queries optimized for performance
- **Performance Comparison**: Side-by-side MongoDB vs Cassandra benchmarking
- **Interactive Charts**: Beautiful Chart.js visualizations
- **Responsive Design**: Modern gradient UI with smooth animations

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │────▶│   Flask     │────▶│  Frontend   │
│  Producer   │     │  Consumer   │     │  Dashboard  │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ├──────▶ MongoDB
                           │
                           └──────▶ Cassandra
```

## 📦 Installation

### Execute the app for the first time:
```bash
py -3.11 -m venv venv
venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
python app.py
```

### Execute the app after:
```bash
.\exec.bat
```

## 📁 Project Structure

```
Web_App/
├── app.py                      # Main Flask application
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables
├── .env.example               # Example environment file
├── exec.bat                   # Quick execution script
├── templates/
│   └── index.html            # Dashboard HTML
├── routes/
│   ├── __init__.py
│   ├── mongo_routes.py       # MongoDB endpoints
│   └── cassandra_routes.py   # Cassandra endpoints
├── db/
│   ├── __init__.py
│   ├── mongo.py             # MongoDB connection
│   └── cassandra.py         # Cassandra connection
├── static/
│   ├── dashboard-part1.js   # Main dashboard logic
│   ├── dashboard-part2.js   # Query handlers
│   └── dashboard-part3.js   # Chart utilities
└── Tests_cassandra/
    ├── test1_exec.sh                           # Monitoring test
    ├── test1_monitoring.sh                     # Monitoring setup script
    ├── test2_exec.sh                           # Data partitioning test
    ├── test2_partitionning.sh                  # Partitioning setup script
    ├── test3_exec.sh                           # Materialized views test
    ├── test3_create_mat_view.sh               # MV creation script
    ├── test4_clustering.sh                     # Clustering/scalability test
    ├── test5_resilance.sh                      # Resilience test
    ├── test6_coherence.sh                      # Data coherence test
    ├── Résultat_test_clustering/              # Test 4 results
    ├── Résultat_test_coherence_doublants/     # Test 6 results
    └── Résultat_test_resilance_cassandra/     # Test 5 results
```

## 🧪 Running Tests

### Test 1: Monitoring and Performance Metrics

**Purpose**: Monitor cluster health, track query performance, and analyze resource utilization.

**Execution**:
```bash
cd Tests_cassandra
# First, set up monitoring infrastructure
bash test1_monitoring.sh

# Then execute the monitoring tests
bash test1_exec.sh
```

**What it tests**:
- Cluster status and node health
- Query execution times and throughput
- CPU and memory utilization per node
- Table statistics and compaction metrics
- Thread pool performance

---

### Test 2: Data Partitioning Strategy

**Purpose**: Evaluate the effectiveness of partition key design and data distribution across nodes.

**Execution**:
```bash
cd Tests_cassandra
# First, configure partitioning schema
bash test2_partitionning.sh

# Then run partitioning tests
bash test2_exec.sh
```

**What it tests**:
- Partition size distribution
- Hot partition detection
- Query performance with different partition keys
- Token range distribution
- Data skew analysis

---

### Test 3: Materialized Views Performance

**Purpose**: Assess the performance impact and benefits of using materialized views for query optimization.

**Execution**:
```bash
cd Tests_cassandra
# First, create materialized views
bash test3_create_mat_view.sh

# Then test MV performance
bash test3_exec.sh
```

**What it tests**:
- Query performance improvement with MVs
- Write amplification cost
- Synchronization lag between base table and MVs
- Storage overhead
- Read vs. write trade-offs

---

### Test 4: Horizontal Scalability (Clustering)

**Purpose**: Evaluate cluster performance as nodes are added from 3 to 6 nodes.

**Execution**:
```bash
cd Tests_cassandra
bash test4_clustering.sh
```

**Results Location**: `Résultat_test_clustering/`

**What it tests**:
- Write throughput scaling (ops/sec)
- Read latency evolution
- CPU and memory usage per node
- Network overhead with increased nodes
- Optimal cluster size determination

**Key Findings**:
- Throughput increased 57.9% from 3 to 6 nodes
- Best performance at 5 nodes (3003.78 ops/sec)
- Latency reduced by 41.6% (28.25ms → 16.51ms at 5 nodes)
- Diminishing returns observed beyond 5 nodes due to coordination overhead

---

### Test 5: Resilience and Fault Tolerance

**Purpose**: Test cluster behavior under node failures and recovery scenarios.

**Execution**:
```bash
cd Tests_cassandra
bash test5_resilance.sh
```

**Results Location**: `Résultat_test_resilance_cassandra/`

**What it tests**:
- Data availability during node failures
- Read/write consistency levels under failure
- Hinted handoff mechanism effectiveness
- Node recovery and data repair
- Quorum behavior with RF=3
- Impact on query latency during failures

**Key Scenarios**:
- Single node failure (1/6 nodes down)
- Multiple node failures (2/6 nodes down)
- Network partition simulation
- Graceful vs. abrupt node shutdown
- Read/write operations during recovery

---

### Test 6: Data Coherence and Duplicate Detection

**Purpose**: Verify data consistency, detect duplicates, and test eventual consistency behavior.

**Execution**:
```bash
cd Tests_cassandra
bash test6_coherence.sh
```

**Results Location**: `Résultat_test_coherence_doublants/`

**What it tests**:
- Duplicate record detection across partitions
- Consistency level impact on data coherence
- Race condition handling in concurrent writes
- Lightweight transaction (LWT) performance
- Read repair effectiveness
- Timestamp-based conflict resolution

**Key Metrics**:
- Duplicate occurrence rate
- Consistency violation frequency
- LWT vs. regular write performance
- Read repair trigger frequency
- Data convergence time after conflicts

---

## 📊 Test Results Summary

All test results are stored in their respective folders with detailed metrics including:
- Performance graphs and charts
- Raw JSON data files
- Statistical analysis reports
- Comparative benchmarks
- Resource utilization logs

### Interpreting Results

Each test folder contains:
- `results_*.json`: Raw test data
- `stats_*.txt`: Statistical summaries
- `tpstats_*.txt`: Thread pool statistics
- Analysis reports in markdown format

---

## 🚀 Getting Started

1. Clone the repository
2. Install dependencies
3. Configure environment variables
4. Start the application with `exec.bat`
5. Access dashboard at `http://localhost:5000`

## 📝 License

MIT License

---

**Happy Analytics!** 🎉