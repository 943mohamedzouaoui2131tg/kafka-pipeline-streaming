# cassandra_routes.py
from flask import Blueprint, request, jsonify
from db.cassandra import session
import time

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
                "sum_passenger_count": row.sum_passenger_count,
                "sum_fare_amount": row.sum_fare_amount,
                "sum_total_amount": row.sum_total_amount
            })
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@cassandra_bp.route("/analytics/top-zones-revenue", methods=["GET"])
def top_zones_revenue():
    """Top zones par revenus (Cassandra)"""
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        
        if not borough or not year_month:
            return jsonify({"error": "borough and year_month required"}), 400
        
        query = """
            SELECT pickup_zone, service_zone, 
                   SUM(sum_total_amount) as total_revenue,
                   SUM(sum_passenger_count) as total_passengers
            FROM trips_by_pickup_zone
            WHERE borough = %s AND year_month = %s
            GROUP BY pickup_zone, service_zone
        """
        
        rows = session.execute(query, (borough, year_month))
        result = [dict(row._asdict()) for row in rows]
        result.sort(key=lambda x: x.get('total_revenue', 0), reverse=True)
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result[:20],
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@cassandra_bp.route("/analytics/hourly-performance", methods=["GET"])
def hourly_performance():
    """Performance par heure (Cassandra)"""
    try:
        start_time = time.time()
        borough = request.args.get("borough")
        year_month = request.args.get("year_month")
        
        if not borough or not year_month:
            return jsonify({"error": "borough and year_month required"}), 400
        
        query = """
            SELECT pickup_hour, 
                   SUM(sum_total_amount) as total_revenue,
                   SUM(sum_passenger_count) as total_passengers
            FROM trips_by_borough_time
            WHERE borough = %s AND year_month = %s
            GROUP BY pickup_hour
        """
        
        rows = session.execute(query, (borough, year_month))
        result = [dict(row._asdict()) for row in rows]
        result.sort(key=lambda x: x.get('pickup_hour', 0))
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@cassandra_bp.route("/analytics/route-stats", methods=["GET"])
def route_stats():
    """Stats par route"""
    try:
        start_time = time.time()
        pickup_location = int(request.args.get("pickup_location_id"))
        year_month = request.args.get("year_month")
        
        if not pickup_location or not year_month:
            return jsonify({"error": "pickup_location_id and year_month required"}), 400
        
        query = """
            SELECT * FROM trips_by_route
            WHERE pickup_location_id = %s AND year_month = %s
            LIMIT 50
        """
        
        rows = session.execute(query, (pickup_location, year_month))
        result = [dict(row._asdict()) for row in rows]
        
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500