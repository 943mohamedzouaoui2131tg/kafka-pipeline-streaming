from flask import Blueprint, request, jsonify
from db.cassandra import session
import time
from collections import defaultdict

cassandra_bp = Blueprint("cassandra", __name__)

@cassandra_bp.route("/trips_by_borough_time", methods=["GET"])
def get_trips_by_borough_time():
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        limit = int(request.args.get("limit", 50))
        
        if borough and year_month:
            query = """
                SELECT * FROM trips_by_borough_time 
                WHERE borough = %s AND year_month = %s
                LIMIT %s
            """
            rows = session.execute(query, (borough, year_month, limit))
        else:
            query = f"SELECT * FROM trips_by_borough_time LIMIT {limit};"
            rows = session.execute(query)
        
        result = []
        for row in rows:
            result.append({
                "borough": row.borough,
                "year_month": row.year_month,
                "pickup_date": str(row.pickup_date),
                "pickup_hour": row.pickup_hour,
                "sum_passenger_count": float(row.sum_passenger_count) if row.sum_passenger_count else 0,
                "sum_fare_amount": float(row.sum_fare_amount) if row.sum_fare_amount else 0,
                "sum_total_amount": float(row.sum_total_amount) if row.sum_total_amount else 0,
                "sum_mta_tax": float(row.sum_mta_tax) if row.sum_mta_tax else 0,
                "sum_tip_amount": float(row.sum_tip_amount) if row.sum_tip_amount else 0,
                "count_vendor_cmt": int(row.count_vendor_cmt) if row.count_vendor_cmt else 0,
                "count_vendor_verifone": int(row.count_vendor_verifone) if row.count_vendor_verifone else 0
            })
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result),
            "success": True
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/cluster/status", methods=["GET"])
def cluster_status():
    """Get Cassandra cluster status with all keyspaces and replication factors"""
    try:
        start_time = time.time()
        
        # Get all keyspaces with their replication strategies
        # Cassandra keyspace names are case-insensitive but stored as lowercase
        keyspaces_query = """
            SELECT keyspace_name, replication 
            FROM system_schema.keyspaces 
            WHERE keyspace_name IN ('projet_bd_rf1', 'projet_bd_rf2', 'projet_bd_rf3')
        """
        keyspaces = list(session.execute(keyspaces_query))
        
        # Get all hosts information
        hosts_info = []
        cluster_metadata = session.cluster.metadata
        
        for host in cluster_metadata.all_hosts():
            try:
                # Ping host to check if it's up
                temp_session = session.cluster.connect()
                temp_session.execute("SELECT now() FROM system.local")
                status = "connected"
            except Exception:
                status = "disconnected"
            
            hosts_info.append({
                "address": str(host.address),
                "datacenter": host.datacenter,
                "rack": host.rack,
                "status": status,
                "is_up": host.is_up
            })
        
        # Process each keyspace
        keyspaces_data = []
        
        for keyspace_row in keyspaces:
            keyspace_name = keyspace_row.keyspace_name
            replication = keyspace_row.replication
            
            # Extract replication factor
            replication_factor = 1
            if 'datacenter1' in replication:
                replication_factor = int(replication['datacenter1'])
            
            # Get tables for this keyspace
            tables_query = """
                SELECT table_name 
                FROM system_schema.tables 
                WHERE keyspace_name = %s
            """
            tables = list(session.execute(tables_query, (keyspace_name,)))
            
            # Get row counts and sizes for each table
            tables_info = []
            total_rows = 0
            
            for table_row in tables:
                table_name = table_row.table_name
                
                try:
                    # Get approximate row count using system tables
                    # Note: This is an estimate, exact counts require full table scan
                    count_query = f"SELECT COUNT(*) as count FROM {keyspace_name}.{table_name} LIMIT 10000"
                    count_result = session.execute(count_query)
                    row_count = count_result.one().count if count_result else 0
                    
                    total_rows += row_count
                    
                    tables_info.append({
                        "name": table_name,
                        "rows": row_count
                    })
                except Exception as table_error:
                    tables_info.append({
                        "name": table_name,
                        "rows": 0,
                        "error": str(table_error)
                    })
            
            keyspaces_data.append({
                "name": keyspace_name,
                "replicationFactor": replication_factor,
                "replicationStrategy": replication.get('class', 'Unknown'),
                "tables": tables_info,
                "tableCount": len(tables_info),
                "totalRows": total_rows,
                "replicas": replication_factor * len(hosts_info)  # Total replicas across nodes
            })
        
        # Calculate total statistics
        total_tables = sum(ks["tableCount"] for ks in keyspaces_data)
        total_rows_all_keyspaces = sum(ks["totalRows"] for ks in keyspaces_data)
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "cluster": {
                "name": cluster_metadata.cluster_name,
                "nodeCount": len(hosts_info),
                "nodes": hosts_info
            },
            "keyspaces": keyspaces_data,
            "totalKeyspaces": len(keyspaces_data),
            "totalTables": total_tables,
            "totalRows": total_rows_all_keyspaces,
            "execution_time_ms": round(execution_time * 1000, 2),
            "success": True
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/cluster/keyspace-details", methods=["GET"])
def keyspace_details():
    """Get detailed information about a specific keyspace"""
    try:
        start_time = time.time()
        keyspace_name = request.args.get("keyspace", "projet_bd_rf3").lower()
        
        valid_keyspaces = ['projet_bd_rf1', 'projet_bd_rf2', 'projet_bd_rf3']
        if keyspace_name not in valid_keyspaces:
            return jsonify({
                "error": f"Invalid keyspace name. Valid options: {valid_keyspaces}",
                "success": False
            }), 400
        
        # Get keyspace replication info
        keyspace_query = """
            SELECT keyspace_name, replication 
            FROM system_schema.keyspaces 
            WHERE keyspace_name = %s
        """
        keyspace_info = session.execute(keyspace_query, (keyspace_name,)).one()
        
        if not keyspace_info:
            return jsonify({
                "error": "Keyspace not found",
                "success": False
            }), 404
        
        replication = keyspace_info.replication
        replication_factor = int(replication.get('datacenter1', 1))
        
        # Get all tables
        tables_query = """
            SELECT table_name 
            FROM system_schema.tables 
            WHERE keyspace_name = %s
        """
        tables = list(session.execute(tables_query, (keyspace_name,)))
        
        # Get detailed info for each table
        tables_details = []
        
        for table_row in tables:
            table_name = table_row.table_name
            
            try:
                # Get row count
                count_query = f"SELECT COUNT(*) as count FROM {keyspace_name}.{table_name} LIMIT 100000"
                count_result = session.execute(count_query)
                row_count = count_result.one().count if count_result else 0
                
                # Get table schema info
                columns_query = """
                    SELECT column_name, type, kind
                    FROM system_schema.columns
                    WHERE keyspace_name = %s AND table_name = %s
                """
                columns = list(session.execute(columns_query, (keyspace_name, table_name)))
                
                partition_keys = [col.column_name for col in columns if col.kind == 'partition_key']
                clustering_keys = [col.column_name for col in columns if col.kind == 'clustering']
                
                tables_details.append({
                    "name": table_name,
                    "rows": row_count,
                    "columnCount": len(columns),
                    "partitionKeys": partition_keys,
                    "clusteringKeys": clustering_keys
                })
                
            except Exception as table_error:
                tables_details.append({
                    "name": table_name,
                    "rows": 0,
                    "error": str(table_error)
                })
        
        # Get node distribution
        cluster_metadata = session.cluster.metadata
        nodes = []
        
        for host in cluster_metadata.all_hosts():
            nodes.append({
                "address": str(host.address),
                "datacenter": host.datacenter,
                "rack": host.rack,
                "status": "up" if host.is_up else "down",
                "hasReplica": True  # All nodes have replicas based on RF
            })
        
        total_rows = sum(table["rows"] for table in tables_details if "rows" in table)
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "keyspace": {
                "name": keyspace_name,
                "replicationFactor": replication_factor,
                "replicationStrategy": replication.get('class', 'Unknown'),
                "replicationDetails": replication
            },
            "tables": tables_details,
            "tableCount": len(tables_details),
            "totalRows": total_rows,
            "nodes": nodes,
            "nodeCount": len(nodes),
            "totalReplicas": replication_factor * len(nodes),
            "execution_time_ms": round(execution_time * 1000, 2),
            "success": True
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/analytics/top-zones-revenue", methods=["GET"])
def top_zones_revenue():
    """Top zones by revenue - aggregates data in Python since Cassandra doesn't support GROUP BY across partitions"""
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        limit = int(request.args.get("limit", 20))
        
        if not borough or not year_month:
            return jsonify({
                "error": "borough and year_month required",
                "success": False
            }), 400
        
        # Query all data for the borough and year_month
        query = """
            SELECT pickup_zone, sum_total_amount, sum_passenger_count
            FROM trips_by_pickup_zone
            WHERE borough = %s AND year_month = %s
        """
        
        rows = session.execute(query, (borough, year_month))
        
        # Aggregate by pickup_zone in Python
        zone_stats = defaultdict(lambda: {"total_revenue": 0, "total_passengers": 0})
        
        for row in rows:
            zone = row.pickup_zone
            zone_stats[zone]["total_revenue"] += float(row.sum_total_amount) if row.sum_total_amount else 0
            zone_stats[zone]["total_passengers"] += float(row.sum_passenger_count) if row.sum_passenger_count else 0
        
        # Convert to list and add zone names
        result = []
        for zone, stats in zone_stats.items():
            result.append({
                "pickup_zone": zone,
                "total_revenue": round(stats["total_revenue"], 2),
                "total_passengers": round(stats["total_passengers"], 0)
            })
        
        # Sort by revenue and limit
        result.sort(key=lambda x: x["total_revenue"], reverse=True)
        result = result[:limit]
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result),
            "success": True
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/analytics/hourly-performance", methods=["GET"])
def hourly_performance():
    """Performance by hour - aggregates data in Python"""
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        
        if not borough or not year_month:
            return jsonify({
                "error": "borough and year_month required",
                "success": False
            }), 400
        
        # Query all data for the borough and year_month
        query = """
            SELECT pickup_hour, sum_total_amount, sum_passenger_count
            FROM trips_by_borough_time
            WHERE borough = %s AND year_month = %s
        """
        
        rows = session.execute(query, (borough, year_month))
        
        # Aggregate by hour in Python
        hour_stats = defaultdict(lambda: {"total_revenue": 0, "total_passengers": 0})
        
        for row in rows:
            hour = row.pickup_hour
            hour_stats[hour]["total_revenue"] += float(row.sum_total_amount) if row.sum_total_amount else 0
            hour_stats[hour]["total_passengers"] += float(row.sum_passenger_count) if row.sum_passenger_count else 0
        
        # Convert to list
        result = []
        for hour in range(24):  # Ensure all hours 0-23 are present
            result.append({
                "pickup_hour": hour,
                "total_revenue": round(hour_stats[hour]["total_revenue"], 2),
                "total_passengers": round(hour_stats[hour]["total_passengers"], 0)
            })
        
        # Sort by hour
        result.sort(key=lambda x: x["pickup_hour"])
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result),
            "success": True
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/analytics/route-stats", methods=["GET"])
def route_stats():
    """Stats by route"""
    try:
        start_time = time.time()
        pickup_location = request.args.get("pickup_location_id")
        year_month = request.args.get("year_month")
        limit = int(request.args.get("limit", 50))
        
        if not pickup_location or not year_month:
            return jsonify({
                "error": "pickup_location_id and year_month required",
                "success": False
            }), 400
        
        pickup_location = int(pickup_location)
        
        query = """
            SELECT * FROM trips_by_route
            WHERE pickup_location_id = %s AND year_month = %s
            LIMIT %s
        """
        
        rows = session.execute(query, (pickup_location, year_month, limit))
        
        result = []
        for row in rows:
            result.append({
                "pickup_location_id": row.pickup_location_id,
                "pickup_zone": row.pickup_zone,
                "dropoff_location_id": row.dropoff_location_id,
                "dropoff_zone": row.dropoff_zone,
                "year_month": row.year_month,
                "pickup_date": str(row.pickup_date),
                "avg_trip_distance": float(row.avg_trip_distance) if row.avg_trip_distance else 0,
                "avg_duration": int(row.avg_duration) if row.avg_duration else 0,
                "avg_fare_amount": float(row.avg_fare_amount) if row.avg_fare_amount else 0,
                "avg_tip_amount": float(row.avg_tip_amount) if row.avg_tip_amount else 0,
                "avg_passenger_count": float(row.avg_passenger_count) if row.avg_passenger_count else 0
            })
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result),
            "success": True
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/analytics/vendor-performance", methods=["GET"])
def vendor_performance():
    """Vendor performance stats"""
    try:
        start_time = time.time()
        vendor_id = request.args.get("vendor_id")
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        limit = int(request.args.get("limit", 50))
        
        if not vendor_id or not borough or not year_month:
            return jsonify({
                "error": "vendor_id, borough and year_month required",
                "success": False
            }), 400
        
        vendor_id = int(vendor_id)
        
        query = """
            SELECT * FROM trips_by_vendor
            WHERE vendor_id = %s AND borough = %s AND year_month = %s
            LIMIT %s
        """
        
        rows = session.execute(query, (vendor_id, borough, year_month, limit))
        
        result = []
        total_revenue = 0
        total_trips = 0
        
        for row in rows:
            revenue = float(row.sum_total_amount) if row.sum_total_amount else 0
            total_revenue += revenue
            total_trips += 1
            
            result.append({
                "vendor_id": row.vendor_id,
                "borough": row.borough,
                "year_month": row.year_month,
                "pickup_date": str(row.pickup_date),
                "pickup_location_id": row.pickup_location_id,
                "sum_passenger_count": float(row.sum_passenger_count) if row.sum_passenger_count else 0,
                "avg_trip_distance": float(row.avg_trip_distance) if row.avg_trip_distance else 0,
                "sum_total_amount": revenue,
                "sum_tip_amount": float(row.sum_tip_amount) if row.sum_tip_amount else 0
            })
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "summary": {
                "total_revenue": round(total_revenue, 2),
                "total_trips": total_trips,
                "avg_revenue_per_trip": round(total_revenue / total_trips, 2) if total_trips > 0 else 0
            },
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result),
            "success": True
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/analytics/payment-types", methods=["GET"])
def payment_types():
    """Payment types distribution"""
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        
        if not borough or not year_month:
            return jsonify({
                "error": "borough and year_month required",
                "success": False
            }), 400
        
        query = """
            SELECT count_payment_card, count_payment_cash, count_payment_free,
                   count_payment_dispute, count_payment_unknown, count_payment_voided
            FROM trips_by_borough_time
            WHERE borough = %s AND year_month = %s
        """
        
        rows = session.execute(query, (borough, year_month))
        
        # Aggregate payment types
        payment_stats = {
            "card": 0,
            "cash": 0,
            "free": 0,
            "dispute": 0,
            "unknown": 0,
            "voided": 0
        }
        
        for row in rows:
            payment_stats["card"] += int(row.count_payment_card) if row.count_payment_card else 0
            payment_stats["cash"] += int(row.count_payment_cash) if row.count_payment_cash else 0
            payment_stats["free"] += int(row.count_payment_free) if row.count_payment_free else 0
            payment_stats["dispute"] += int(row.count_payment_dispute) if row.count_payment_dispute else 0
            payment_stats["unknown"] += int(row.count_payment_unknown) if row.count_payment_unknown else 0
            payment_stats["voided"] += int(row.count_payment_voided) if row.count_payment_voided else 0
        
        total = sum(payment_stats.values())
        
        result = []
        for payment_type, count in payment_stats.items():
            result.append({
                "payment_type": payment_type,
                "count": count,
                "percentage": round((count / total * 100), 2) if total > 0 else 0
            })
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "total_transactions": total,
            "execution_time_ms": round(execution_time * 1000, 2),
            "success": True
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@cassandra_bp.route("/analytics/anomaly-detection", methods=["GET"])
def anomaly_detection():
    """Detect anomalous trips in Cassandra data with clear categorization"""
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        limit = int(request.args.get("limit", 100))
        
        if not borough or not year_month:
            return jsonify({
                "error": "borough and year_month parameters are required",
                "example": "/analytics/anomaly-detection?borough=Manhattan&year_month=2013-01",
                "available_boroughs": ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"],
                "success": False
            }), 400
        
        anomalies = []
        type_counts = {
            "zero_distance": 0,
            "invalid_duration": 0,
            "invalid_amount": 0,
            "high_speed": 0,
            "other": 0
        }
        
        # 1. Check trips_by_borough_time for financial anomalies
        query1 = """
            SELECT borough, year_month, pickup_date, pickup_hour,
                   sum_passenger_count, sum_fare_amount, sum_total_amount,
                   sum_mta_tax, sum_tip_amount
            FROM trips_by_borough_time
            WHERE borough = %s AND year_month = %s
        """
        
        rows = session.execute(query1, (borough, year_month))
        
        for row in rows:
            total_amount = float(row.sum_total_amount) if row.sum_total_amount else 0
            fare_amount = float(row.sum_fare_amount) if row.sum_fare_amount else 0
            tip_amount = float(row.sum_tip_amount) if row.sum_tip_amount else 0
            
            # Check for invalid amounts
            if total_amount <= 0 or fare_amount < 0 or tip_amount < 0:
                anomaly_type = "invalid_amount"
                type_counts[anomaly_type] += 1
                
                anomalies.append({
                    "pickup_zone": f"{row.borough} (Hour {row.pickup_hour})",
                    "dropoff_zone": "Unknown",
                    "borough": row.borough,
                    "pickup_datetime": f"{row.pickup_date} {row.pickup_hour:02d}:00:00",
                    "distance_km": 0,
                    "distance_miles": 0,
                    "duration_minutes": 0,
                    "total_amount": round(total_amount, 2),
                    "fare_amount": round(fare_amount, 2),
                    "tip_amount": round(tip_amount, 2),
                    "speed_kmh": 0,
                    "speed_mph": 0,
                    "anomaly_type": anomaly_type,
                    "source": "trips_by_borough_time"
                })
                
                if len(anomalies) >= limit:
                    break
        
        # 2. Check trips_by_pickup_zone for distance anomalies
        if len(anomalies) < limit:
            query2 = """
                SELECT borough, year_month, pickup_date, pickup_hour, pickup_zone,
                       avg_trip_distance, sum_fare_amount, sum_total_amount
                FROM trips_by_pickup_zone
                WHERE borough = %s AND year_month = %s
                LIMIT 500
            """
            
            zone_rows = session.execute(query2, (borough, year_month))
            
            for row in zone_rows:
                if len(anomalies) >= limit:
                    break
                
                distance = float(row.avg_trip_distance) if row.avg_trip_distance else 0
                total_amount = float(row.sum_total_amount) if row.sum_total_amount else 0
                
                # Zero distance with charges
                if distance == 0 and total_amount > 0:
                    anomaly_type = "zero_distance"
                    type_counts[anomaly_type] += 1
                    
                    anomalies.append({
                        "pickup_zone": row.pickup_zone or "Unknown",
                        "dropoff_zone": "Unknown",
                        "borough": row.borough,
                        "pickup_datetime": f"{row.pickup_date} {row.pickup_hour:02d}:00:00",
                        "distance_km": 0,
                        "distance_miles": 0,
                        "duration_minutes": 0,
                        "total_amount": round(total_amount, 2),
                        "fare_amount": round(float(row.sum_fare_amount) if row.sum_fare_amount else 0, 2),
                        "tip_amount": 0,
                        "speed_kmh": 0,
                        "speed_mph": 0,
                        "anomaly_type": anomaly_type,
                        "source": "trips_by_pickup_zone"
                    })
        
        # 3. Check trips_by_route for speed and duration anomalies
        if len(anomalies) < limit:
            # Sample some pickup locations
            try:
                location_query = """
                    SELECT DISTINCT pickup_location_id
                    FROM trips_by_route
                    WHERE year_month = %s
                    LIMIT 10
                    ALLOW FILTERING
                """
                
                locations = list(session.execute(location_query, (year_month,)))
                
                for loc in locations:
                    if len(anomalies) >= limit:
                        break
                    
                    route_query = """
                        SELECT pickup_location_id, pickup_zone, dropoff_location_id, 
                               dropoff_zone, year_month, pickup_date,
                               avg_trip_distance, avg_duration, avg_fare_amount
                        FROM trips_by_route
                        WHERE pickup_location_id = %s AND year_month = %s
                        LIMIT 50
                    """
                    
                    route_rows = session.execute(route_query, (loc.pickup_location_id, year_month))
                    
                    for row in route_rows:
                        if len(anomalies) >= limit:
                            break
                        
                        distance_miles = float(row.avg_trip_distance) if row.avg_trip_distance else 0
                        distance_km = distance_miles * 1.60934
                        duration_seconds = int(row.avg_duration) if row.avg_duration else 0
                        duration_minutes = duration_seconds / 60
                        
                        anomaly_type = None
                        
                        # Invalid duration
                        if duration_seconds <= 0 and distance_miles > 0:
                            anomaly_type = "invalid_duration"
                        
                        # Calculate speed if we have valid data
                        elif distance_miles > 0 and duration_seconds > 0:
                            speed_mph = (distance_miles / duration_seconds) * 3600
                            speed_kmh = speed_mph * 1.60934
                            
                            # High speed (>90 mph / 145 kmh in city)
                            if speed_mph > 90:
                                anomaly_type = "high_speed"
                        
                        if anomaly_type:
                            type_counts[anomaly_type] += 1
                            
                            speed_mph = (distance_miles / duration_seconds) * 3600 if duration_seconds > 0 else 0
                            speed_kmh = speed_mph * 1.60934
                            
                            anomalies.append({
                                "pickup_zone": row.pickup_zone or "Unknown",
                                "dropoff_zone": row.dropoff_zone or "Unknown",
                                "borough": "Unknown",
                                "pickup_datetime": str(row.pickup_date),
                                "distance_km": round(distance_km, 2),
                                "distance_miles": round(distance_miles, 2),
                                "duration_minutes": round(duration_minutes, 2),
                                "total_amount": round(float(row.avg_fare_amount) if row.avg_fare_amount else 0, 2),
                                "fare_amount": round(float(row.avg_fare_amount) if row.avg_fare_amount else 0, 2),
                                "tip_amount": 0,
                                "speed_kmh": round(speed_kmh, 2),
                                "speed_mph": round(speed_mph, 2),
                                "anomaly_type": anomaly_type,
                                "source": "trips_by_route"
                            })
            
            except Exception as route_error:
                print(f"Route query error: {route_error}")
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "data": anomalies[:limit],
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(anomalies[:limit]),
            "total_found": len(anomalies),
            "filters": {
                "borough": borough,
                "year_month": year_month,
                "limit": limit
            },
            "anomaly_counts": type_counts,
            "summary": {
                "zero_distance": type_counts["zero_distance"],
                "invalid_duration": type_counts["invalid_duration"],
                "invalid_amount": type_counts["invalid_amount"],
                "high_speed": type_counts["high_speed"],
                "other": type_counts["other"]
            },
            "success": True
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500