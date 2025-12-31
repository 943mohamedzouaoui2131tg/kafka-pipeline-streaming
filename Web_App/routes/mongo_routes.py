# mongo_routes.py
from flask import Blueprint, request, jsonify
from db.mongo import collection
from bson.objectid import ObjectId
import time
from datetime import datetime

mongo_bp = Blueprint("mongo", __name__)

@mongo_bp.route("/trips", methods=["GET"])
def get_trips():
    try:
        start_time = time.time()
        docs = collection.find().limit(50)
        result = []
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            result.append(doc)
        execution_time = time.time() - start_time
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@mongo_bp.route("/analytics/top-zones-by-revenue", methods=["GET"])
def top_zones_by_revenue():
    """Top zones par revenus"""
    try:
        start_time = time.time()
        limit = int(request.args.get("limit", 10))
        
        pipeline = [
            {
                "$group": {
                    "_id": "$PULocationID.zone",
                    "borough": {"$first": "$PULocationID.borough"},
                    "total_revenue": {"$sum": "$total_amount"},
                    "total_trips": {"$count": {}},
                    "avg_fare": {"$avg": "$fare_amount"}
                }
            },
            {"$sort": {"total_revenue": -1}},
            {"$limit": limit}
        ]
        
        result = list(collection.aggregate(pipeline))
        execution_time = time.time() - start_time
        
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@mongo_bp.route("/analytics/top-zones-by-trips", methods=["GET"])
def top_zones_by_trips():
    """Top zones par volume de trajets"""
    try:
        start_time = time.time()
        limit = int(request.args.get("limit", 10))
        
        pipeline = [
            {
                "$group": {
                    "_id": "$PULocationID.zone",
                    "borough": {"$first": "$PULocationID.borough"},
                    "total_trips": {"$count": {}},
                    "total_revenue": {"$sum": "$total_amount"},
                    "avg_passengers": {"$avg": "$passenger_count"}
                }
            },
            {"$sort": {"total_trips": -1}},
            {"$limit": limit}
        ]
        
        result = list(collection.aggregate(pipeline))
        execution_time = time.time() - start_time
        
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@mongo_bp.route("/analytics/passenger-distribution", methods=["GET"])
def passenger_distribution():
    """Distribution des nombre de passagers"""
    try:
        start_time = time.time()
        
        pipeline = [
            {
                "$group": {
                    "_id": "$passenger_count",
                    "count": {"$count": {}},
                    "avg_fare": {"$avg": "$fare_amount"},
                    "avg_distance": {"$avg": "$trip_distance"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        result = list(collection.aggregate(pipeline))
        execution_time = time.time() - start_time
        
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@mongo_bp.route("/analytics/anomaly-detection", methods=["GET"])
def anomaly_detection():
    """Détection de trips anormaux (distance nulle, vitesse excessive)"""
    try:
        start_time = time.time()
        
        pipeline = [
            {
                "$addFields": {
                    "pickup_dt": {
                        "$dateFromString": {
                            "dateString": "$tpep_pickup_datetime",
                            "format": "%Y-%m-%d %H:%M:%S"
                        }
                    },
                    "dropoff_dt": {
                        "$dateFromString": {
                            "dateString": "$tpep_dropoff_datetime",
                            "format": "%Y-%m-%d %H:%M:%S"
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "duration_minutes": {
                        "$divide": [
                            {"$subtract": ["$dropoff_dt", "$pickup_dt"]},
                            60000
                        ]
                    }
                }
            },
            {
                "$match": {
                    "$or": [
                        {"trip_distance": 0},
                        {"duration_minutes": {"$lte": 0}},
                        {"total_amount": {"$lte": 0}},
                        {
                            "$expr": {
                                "$gt": [
                                    {"$divide": ["$trip_distance", {"$divide": ["$duration_minutes", 60]}]},
                                    100
                                ]
                            }
                        }
                    ]
                }
            },
            {"$limit": 100}
        ]
        
        result = list(collection.aggregate(pipeline))
        for doc in result:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@mongo_bp.route("/analytics/hourly-stats", methods=["GET"])
def hourly_stats():
    """Stats par heure"""
    try:
        start_time = time.time()
        
        pipeline = [
            {
                "$addFields": {
                    "pickup_dt": {
                        "$dateFromString": {
                            "dateString": "$tpep_pickup_datetime",
                            "format": "%Y-%m-%d %H:%M:%S"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {"$hour": "$pickup_dt"},
                    "total_trips": {"$count": {}},
                    "total_revenue": {"$sum": "$total_amount"},
                    "avg_fare": {"$avg": "$fare_amount"},
                    "avg_distance": {"$avg": "$trip_distance"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        result = list(collection.aggregate(pipeline))
        execution_time = time.time() - start_time
        
        return jsonify({
            "data": result,
            "execution_time_ms": round(execution_time * 1000, 2),
            "count": len(result)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


