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

# Execute the app for the first time :
```bash
py -3.11 -m venv venv
venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
python app.py
```
# Execute the app after :
.\exec.bat

### Step 3: Project Structure

Ensure your project has this structure:

```
your_project/
├── app.py                      # Main Flask application
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables
├── .env.example               # Example environment file
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
└── Data/
    └── datasets_json/
        └── test.json        # Test data
```
**Happy Analytics!** 🎉