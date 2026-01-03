# cassandra_routes.py
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