from dotenv import load_dotenv
import os
from fastapi import FastAPI, HTTPException
from typing import List, Dict
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

# FastAPI app
app = FastAPI()

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT'))
}
# Helper function to connect to the database
def get_db_connection():
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# Endpoint: /recommendations/<ADV>/<Modelo>
@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: int, model: str):
    connection = get_db_connection()
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT product_id, metric, date
        FROM recommendations
        WHERE advertiser_id = %s AND model = %s AND date = %s;
        """
        today = datetime.now().date()
        cursor.execute(query, (adv, model, today))
        recommendations = cursor.fetchall()
        if not recommendations:
            raise HTTPException(status_code=404, detail="No recommendations found for today.")
        return {"recommendations": recommendations}
    finally:
        connection.close()

# Endpoint: /stats/
@app.get("/stats/")
def get_stats():
    connection = get_db_connection()
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Query 1: Number of advertisers
        cursor.execute("SELECT COUNT(DISTINCT advertiser_id) AS advertiser_count FROM recommendations;")
        advertiser_count = cursor.fetchone()["advertiser_count"]

        # Query 2: Advertisers with the most daily recommendation changes
        cursor.execute("""
        SELECT advertiser_id, COUNT(DISTINCT date) AS change_count
        FROM recommendations
        GROUP BY advertiser_id
        ORDER BY change_count DESC
        LIMIT 5;
        """)
        top_changing_advertisers = cursor.fetchall()

        # Query 3: Matching statistics between models
        cursor.execute("""
        SELECT r1.advertiser_id, COUNT(*) AS match_count
        FROM recommendations r1
        JOIN recommendations r2
        ON r1.advertiser_id = r2.advertiser_id
        AND r1.product_id = r2.product_id
        AND r1.date = r2.date
        AND r1.model != r2.model
        GROUP BY r1.advertiser_id
        ORDER BY match_count DESC;
        """)
        matching_stats = cursor.fetchall()

        return {
            "advertiser_count": advertiser_count,
            "top_changing_advertisers": top_changing_advertisers,
            "matching_stats": matching_stats
        }
    finally:
        connection.close()

# Endpoint: /history/<ADV>/
@app.get("/history/{adv}")
def get_history(adv: int):
    connection = get_db_connection()
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT product_id, metric, date, model
        FROM recommendations
        WHERE advertiser_id = %s AND date >= %s
        ORDER BY date DESC;
        """
        seven_days_ago = (datetime.now() - timedelta(days=7)).date()
        cursor.execute(query, (adv, seven_days_ago))
        history = cursor.fetchall()
        if not history:
            raise HTTPException(status_code=404, detail="No history found for the advertiser in the last 7 days.")
        return {"history": history}
    finally:
        connection.close()
