# app.py
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

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-change-this')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

app.register_blueprint(mongo_bp, url_prefix="/mongo")
app.register_blueprint(cassandra_bp, url_prefix="/cassandra")

# Kafka configuration
KAFKA_BROKER1 = os.getenv("KAFKA_BROKER1", "localhost:9092")
KAFKA_BROKER2 = os.getenv("KAFKA_BROKER2", "localhost:9093")
KAFKA_TOPIC = 'taxi_raw'

# Streaming stats
streaming_stats = {
    "total_trips": 0,
    "mongo_inserts": 0,
    "cassandra_inserts": 0,
    "last_trip_time": None,
    "recent_trips": [],
    "trips_per_minute": 0,
    "total_revenue": 0,
    "connected": False,
    "errors": 0,
    "start_time": None
}

# Track trips for rate calculation
trip_timestamps = []

# Control flag for consumer thread
consumer_running = True

def calculate_trips_per_minute():
    """Calculate trips per minute based on recent timestamps"""
    global trip_timestamps
    current_time = time.time()
    # Keep only timestamps from last minute
    trip_timestamps = [ts for ts in trip_timestamps if current_time - ts < 60]
    return len(trip_timestamps)

def kafka_consumer_thread():
    """Background thread to consume Kafka messages and emit to frontend"""
    global streaming_stats, trip_timestamps, consumer_running
    
    print(f"Starting Kafka consumer for topic: {KAFKA_TOPIC}")
    print(f"Connecting to brokers: {KAFKA_BROKER1}, {KAFKA_BROKER2}")
    
    consumer = None
    retry_count = 0
    max_retries = 10
    
    while consumer_running and retry_count < max_retries:
        try:
            # Use 'earliest' to read all messages from beginning, or 'latest' for only new messages
            # Change to 'earliest' if you want to process all existing messages
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER1, KAFKA_BROKER2],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                auto_offset_reset='earliest',  # Changed to 'earliest' to read all messages
                enable_auto_commit=True,
                group_id='taxi-analytics-dashboard-v1',  # Change group_id to start fresh
                consumer_timeout_ms=5000,  # Increased timeout
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=100,
                max_poll_interval_ms=300000
            )
            
            streaming_stats["connected"] = True
            streaming_stats["start_time"] = datetime.now()
            print("✅ Kafka consumer connected successfully!")
            socketio.emit('kafka_status', {'status': 'connected', 'message': 'Kafka consumer connected'})
            
            # Reset retry count on successful connection
            retry_count = 0
            
            # Continuous polling loop
            while consumer_running:
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000, max_records=100)
                    
                    if not message_batch:
                        # No messages, continue polling
                        continue
                    
                    # Process all messages in batch
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                trip_data = message.value
                                current_time = time.time()
                                
                                # Update statistics
                                streaming_stats["total_trips"] += 1
                                streaming_stats["last_trip_time"] = current_time
                                trip_timestamps.append(current_time)
                                
                                # Calculate revenue - handle None values
                                total_amount = trip_data.get("total_amount")
                                if total_amount is not None:
                                    try:
                                        streaming_stats["total_revenue"] += float(total_amount)
                                    except (ValueError, TypeError):
                                        pass
                                
                                # Update trips per minute
                                streaming_stats["trips_per_minute"] = calculate_trips_per_minute()
                                
                                # Extract relevant info for display
                                pu_location = trip_data.get("PULocationID", {})
                                do_location = trip_data.get("DOLocationID", {})
                                
                                pickup_zone = pu_location.get("zone", "Unknown") if isinstance(pu_location, dict) else "Unknown"
                                dropoff_zone = do_location.get("zone", "Unknown") if isinstance(do_location, dict) else "Unknown"
                                borough = pu_location.get("borough", "Unknown") if isinstance(pu_location, dict) else "Unknown"
                                
                                recent_trip = {
                                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                                    "trip_id": trip_data.get("trip_id", "N/A"),
                                    "pickup_zone": pickup_zone,
                                    "dropoff_zone": dropoff_zone,
                                    "borough": borough,
                                    "amount": float(total_amount) if total_amount is not None else 0,
                                    "distance": float(trip_data.get("trip_distance", 0)),
                                    "passengers": int(trip_data.get("passenger_count", 0))
                                }
                                
                                # Add to recent trips (keep last 15)
                                streaming_stats["recent_trips"].insert(0, recent_trip)
                                if len(streaming_stats["recent_trips"]) > 15:
                                    streaming_stats["recent_trips"].pop()
                                
                                # Emit to all connected clients every message
                                socketio.emit('new_trip', {
                                    "trip": recent_trip,
                                    "stats": {
                                        "total_trips": streaming_stats["total_trips"],
                                        "trips_per_minute": streaming_stats["trips_per_minute"],
                                        "total_revenue": round(streaming_stats["total_revenue"], 2),
                                        "recent_trips": streaming_stats["recent_trips"][:10]
                                    }
                                })
                                
                                # Log every 10 trips
                                if streaming_stats["total_trips"] % 10 == 0:
                                    print(f"📊 Processed {streaming_stats['total_trips']} trips | "
                                          f"Rate: {streaming_stats['trips_per_minute']}/min | "
                                          f"Revenue: ${streaming_stats['total_revenue']:.2f}")
                                
                            except Exception as e:
                                streaming_stats["errors"] += 1
                                print(f"❌ Error processing message: {e}")
                                socketio.emit('kafka_error', {'error': str(e)})
                
                except Exception as poll_error:
                    print(f"⚠️ Poll error: {poll_error}")
                    time.sleep(1)
                    continue
            
            # Graceful shutdown
            print("🛑 Consumer loop stopped")
            
        except KafkaError as e:
            retry_count += 1
            streaming_stats["connected"] = False
            print(f"❌ Kafka connection error (attempt {retry_count}/{max_retries}): {e}")
            socketio.emit('kafka_status', {
                'status': 'disconnected', 
                'message': f'Connection error: {e}',
                'retry': retry_count
            })
            
            if retry_count < max_retries and consumer_running:
                wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30s
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Max retries reached. Kafka consumer failed.")
                break
                
        except Exception as e:
            print(f"❌ Unexpected error in Kafka consumer: {e}")
            import traceback
            traceback.print_exc()
            streaming_stats["connected"] = False
            socketio.emit('kafka_status', {'status': 'error', 'message': str(e)})
            break
        
        finally:
            if consumer:
                try:
                    consumer.close()
                    print("✅ Kafka consumer closed gracefully")
                except Exception as e:
                    print(f"⚠️ Error closing consumer: {e}")

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/streaming-stats")
def get_streaming_stats():
    """API endpoint to get current streaming statistics"""
    uptime = None
    if streaming_stats["start_time"]:
        uptime = str(datetime.now() - streaming_stats["start_time"]).split('.')[0]
    
    return jsonify({
        "total_trips": streaming_stats["total_trips"],
        "trips_per_minute": streaming_stats["trips_per_minute"],
        "total_revenue": round(streaming_stats["total_revenue"], 2),
        "connected": streaming_stats["connected"],
        "errors": streaming_stats["errors"],
        "recent_trips": streaming_stats["recent_trips"][:5],
        "uptime": uptime
    })

@app.route("/api/health")
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy" if streaming_stats["connected"] else "degraded",
        "kafka_connected": streaming_stats["connected"],
        "total_trips_processed": streaming_stats["total_trips"],
        "errors": streaming_stats["errors"],
        "total_revenue": round(streaming_stats["total_revenue"], 2)
    })

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print(f"🔌 Client connected")
    emit('connection_response', {
        'status': 'connected',
        'message': 'Connected to streaming service',
        'kafka_status': 'connected' if streaming_stats["connected"] else 'disconnected',
        'stats': {
            "total_trips": streaming_stats["total_trips"],
            "trips_per_minute": streaming_stats["trips_per_minute"],
            "total_revenue": round(streaming_stats["total_revenue"], 2),
            "recent_trips": streaming_stats["recent_trips"][:5]
        }
    })

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print(f"🔌 Client disconnected")

@socketio.on('request_stats')
def handle_stats_request():
    """Handle manual stats request from client"""
    emit('stats_update', {
        "total_trips": streaming_stats["total_trips"],
        "trips_per_minute": streaming_stats["trips_per_minute"],
        "total_revenue": round(streaming_stats["total_revenue"], 2),
        "recent_trips": streaming_stats["recent_trips"][:10]
    })

def cleanup():
    """Cleanup function for graceful shutdown"""
    global consumer_running
    consumer_running = False
    print("🧹 Cleaning up...")

if __name__ == "__main__":
    print("🚀 Starting NYC Taxi Analytics Dashboard...")
    print(f"📡 Kafka brokers: {KAFKA_BROKER1}, {KAFKA_BROKER2}")
    print(f"📊 Topic: {KAFKA_TOPIC}")
    print(f"📖 Reading from: earliest (all messages)")
    
    # Start Kafka consumer in background thread
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()
    
    print("🌐 Starting Flask server on http://localhost:5000")
    
    try:
        socketio.run(app, debug=True, host='localhost', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down...")
        cleanup()
    finally:
        print("✅ Application stopped")