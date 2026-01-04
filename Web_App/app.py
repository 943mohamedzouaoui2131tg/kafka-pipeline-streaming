# app.py - Optimized with non-blocking operations
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from routes.mongo_routes import mongo_bp
from routes.cassandra_routes import cassandra_bp
import threading
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
from datetime import datetime
import os
from dotenv import load_dotenv
from collections import deque

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-change-this')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=120, ping_interval=25)

app.register_blueprint(mongo_bp, url_prefix="/mongo")
app.register_blueprint(cassandra_bp, url_prefix="/cassandra")

# Kafka configuration
KAFKA_BROKER1 = os.getenv("KAFKA_BROKER1", "localhost:9092")
KAFKA_BROKER2 = os.getenv("KAFKA_BROKER2", "localhost:9093")
KAFKA_TOPIC = 'taxi_raw'

# Streaming stats with thread-safe updates
streaming_stats = {
    "total_trips": 0,
    "mongo_inserts": 0,
    "cassandra_inserts": 0,
    "last_trip_time": None,
    "recent_trips": deque(maxlen=15),
    "trips_per_minute": 0,
    "total_revenue": 0.0,
    "connected": False,
    "errors": 0,
    "start_time": None,
    "last_update": time.time()
}

# Thread lock for stats
stats_lock = threading.Lock()

# Track trips for rate calculation
trip_timestamps = deque(maxlen=1000)

# Control flag
consumer_running = True

def calculate_trips_per_minute():
    """Calculate trips per minute based on recent timestamps"""
    current_time = time.time()
    # Count timestamps from last minute
    recent = [ts for ts in trip_timestamps if current_time - ts < 60]
    return len(recent)

def kafka_consumer_thread():
    """Optimized Kafka consumer thread"""
    global streaming_stats, trip_timestamps, consumer_running
    
    print(f"🎧 Starting Kafka consumer for topic: {KAFKA_TOPIC}")
    print(f"📡 Connecting to brokers: {KAFKA_BROKER1}, {KAFKA_BROKER2}")
    
    consumer = None
    retry_count = 0
    max_retries = 5
    
    while consumer_running and retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER1, KAFKA_BROKER2],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8', errors='replace') if m else None,
                auto_offset_reset='latest',  # Changed to latest to avoid reprocessing
                enable_auto_commit=True,
                group_id='taxi-analytics-dashboard-v3',
                consumer_timeout_ms=10000,
                session_timeout_ms=60000,
                heartbeat_interval_ms=20000,
                max_poll_records=500,
                max_poll_interval_ms=600000,
                fetch_min_bytes=1024,
                fetch_max_wait_ms=500
            )
            
            with stats_lock:
                streaming_stats["connected"] = True
                streaming_stats["start_time"] = datetime.now()
            
            print("✅ Kafka consumer connected successfully!")
            socketio.emit('kafka_status', {'status': 'connected', 'message': 'Connected'})
            
            retry_count = 0
            batch_count = 0
            last_emit = time.time()
            
            while consumer_running:
                try:
                    # Poll with shorter timeout for responsiveness
                    message_batch = consumer.poll(timeout_ms=100, max_records=100)
                    
                    if not message_batch:
                        # Emit stats periodically even without new messages
                        if time.time() - last_emit > 2:
                            emit_current_stats()
                            last_emit = time.time()
                        continue
                    
                    # Process messages quickly
                    trip_count = 0
                    total_revenue = 0.0
                    recent_trips_batch = []
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                trip_data = message.value
                                trip_count += 1
                                
                                # Quick revenue calculation
                                total_amount = trip_data.get("total_amount", 0)
                                if total_amount:
                                    try:
                                        total_revenue += float(total_amount)
                                    except (ValueError, TypeError):
                                        pass
                                
                                # Store only last few trips for display
                                if len(recent_trips_batch) < 5:
                                    pu_loc = trip_data.get("PULocationID", {})
                                    do_loc = trip_data.get("DOLocationID", {})
                                    
                                    recent_trips_batch.append({
                                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                                        "trip_id": trip_data.get("trip_id", "N/A"),
                                        "pickup_zone": pu_loc.get("zone", "Unknown") if isinstance(pu_loc, dict) else "Unknown",
                                        "dropoff_zone": do_loc.get("zone", "Unknown") if isinstance(do_loc, dict) else "Unknown",
                                        "borough": pu_loc.get("borough", "Unknown") if isinstance(pu_loc, dict) else "Unknown",
                                        "amount": float(total_amount) if total_amount else 0,
                                        "distance": float(trip_data.get("trip_distance", 0)),
                                        "passengers": int(trip_data.get("passenger_count", 0))
                                    })
                                
                            except Exception as e:
                                with stats_lock:
                                    streaming_stats["errors"] += 1
                    
                    # Batch update stats
                    current_time = time.time()
                    with stats_lock:
                        streaming_stats["total_trips"] += trip_count
                        streaming_stats["mongo_inserts"] = streaming_stats["total_trips"]
                        streaming_stats["cassandra_inserts"] = streaming_stats["total_trips"]
                        streaming_stats["total_revenue"] += total_revenue
                        streaming_stats["last_trip_time"] = current_time
                        
                        # Add timestamps for rate calculation
                        for _ in range(trip_count):
                            trip_timestamps.append(current_time)
                        
                        streaming_stats["trips_per_minute"] = calculate_trips_per_minute()
                        
                        # Update recent trips
                        for trip in recent_trips_batch:
                            streaming_stats["recent_trips"].appendleft(trip)
                    
                    batch_count += 1
                    
                    # Emit to clients every 50 batches or every 5 seconds
                    if batch_count % 50 == 0 or (time.time() - last_emit > 5):
                        emit_current_stats()
                        last_emit = time.time()
                        
                        if batch_count % 100 == 0:
                            with stats_lock:
                                print(f"📊 Processed {streaming_stats['total_trips']} trips | "
                                      f"Rate: {streaming_stats['trips_per_minute']}/min | "
                                      f"Revenue: ${streaming_stats['total_revenue']:.2f}")
                
                except Exception as poll_error:
                    print(f"⚠️ Poll error: {poll_error}")
                    time.sleep(0.1)
                    continue
            
            print("🛑 Consumer loop stopped")
            
        except KafkaError as e:
            retry_count += 1
            with stats_lock:
                streaming_stats["connected"] = False
            
            print(f"❌ Kafka error (attempt {retry_count}/{max_retries}): {e}")
            socketio.emit('kafka_status', {
                'status': 'disconnected',
                'message': f'Connection error: {e}',
                'retry': retry_count
            })
            
            if retry_count < max_retries and consumer_running:
                wait_time = min(2 ** retry_count, 30)
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Max retries reached")
                break
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            with stats_lock:
                streaming_stats["connected"] = False
            socketio.emit('kafka_status', {'status': 'error', 'message': str(e)})
            break
        
        finally:
            if consumer:
                try:
                    consumer.close()
                    print("✅ Kafka consumer closed")
                except Exception as e:
                    print(f"⚠️ Error closing consumer: {e}")

def emit_current_stats():
    """Emit current stats to all connected clients"""
    with stats_lock:
        stats_copy = {
            "total_trips": streaming_stats["total_trips"],
            "mongo_inserts": streaming_stats["mongo_inserts"],
            "cassandra_inserts": streaming_stats["cassandra_inserts"],
            "trips_per_minute": streaming_stats["trips_per_minute"],
            "total_revenue": round(streaming_stats["total_revenue"], 2),
            "recent_trips": list(streaming_stats["recent_trips"])[:10]
        }
    
    socketio.emit('new_trip', {"stats": stats_copy})

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/streaming-stats")
def get_streaming_stats():
    """Get current streaming statistics"""
    with stats_lock:
        uptime = None
        if streaming_stats["start_time"]:
            uptime = str(datetime.now() - streaming_stats["start_time"]).split('.')[0]
        
        return jsonify({
            "total_trips": streaming_stats["total_trips"],
            "mongo_inserts": streaming_stats["mongo_inserts"],
            "cassandra_inserts": streaming_stats["cassandra_inserts"],
            "trips_per_minute": streaming_stats["trips_per_minute"],
            "total_revenue": round(streaming_stats["total_revenue"], 2),
            "connected": streaming_stats["connected"],
            "errors": streaming_stats["errors"],
            "recent_trips": list(streaming_stats["recent_trips"])[:5],
            "uptime": uptime,
            "success": True
        })

@app.route("/api/health")
def health_check():
    """Health check endpoint"""
    with stats_lock:
        return jsonify({
            "status": "healthy" if streaming_stats["connected"] else "degraded",
            "kafka_connected": streaming_stats["connected"],
            "total_trips_processed": streaming_stats["total_trips"],
            "errors": streaming_stats["errors"],
            "total_revenue": round(streaming_stats["total_revenue"], 2),
            "success": True
        })

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print(f"🔌 Client connected")
    with stats_lock:
        emit('connection_response', {
            'status': 'connected',
            'message': 'Connected to streaming service',
            'kafka_status': 'connected' if streaming_stats["connected"] else 'disconnected',
            'stats': {
                "total_trips": streaming_stats["total_trips"],
                "mongo_inserts": streaming_stats["mongo_inserts"],
                "cassandra_inserts": streaming_stats["cassandra_inserts"],
                "trips_per_minute": streaming_stats["trips_per_minute"],
                "total_revenue": round(streaming_stats["total_revenue"], 2),
                "recent_trips": list(streaming_stats["recent_trips"])[:5]
            }
        })

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print(f"🔌 Client disconnected")

@socketio.on('request_stats')
def handle_stats_request():
    """Handle manual stats request"""
    with stats_lock:
        emit('stats_update', {
            "total_trips": streaming_stats["total_trips"],
            "mongo_inserts": streaming_stats["mongo_inserts"],
            "cassandra_inserts": streaming_stats["cassandra_inserts"],
            "trips_per_minute": streaming_stats["trips_per_minute"],
            "total_revenue": round(streaming_stats["total_revenue"], 2),
            "recent_trips": list(streaming_stats["recent_trips"])[:10]
        })

def cleanup():
    """Cleanup function"""
    global consumer_running
    consumer_running = False
    print("🧹 Cleaning up...")

if __name__ == "__main__":
    print("🚀 Starting NYC Taxi Analytics Dashboard...")
    print(f"📡 Kafka brokers: {KAFKA_BROKER1}, {KAFKA_BROKER2}")
    print(f"📊 Topic: {KAFKA_TOPIC}")
    print(f"📖 Reading from: latest (new messages only)")
    print("=" * 70)
    
    # Start Kafka consumer in background thread
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True, name="KafkaConsumer")
    kafka_thread.start()
    
    print("🌐 Starting Flask server on http://localhost:5000")
    print("=" * 70)
    
    try:
        socketio.run(app, debug=False, host='localhost', port=5000, use_reloader=False, log_output=False)
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down...")
        cleanup()
    finally:
        print("✅ Application stopped")