import os
import json
import time
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "port_kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")

DB_HOST = os.getenv("DB_HOST", "port_postgres_db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "portfolio")
DB_USER = os.getenv("DB_USER", "gyumin")
DB_PASS = os.getenv("DB_PASS", "03040228")

time.sleep(15) # Wait for Kafka and DB to be ready

# Connect to Database
def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return conn
        except Exception as e:
            print(f"Waiting for Database... {e}")
            time.sleep(5)

conn = get_db_connection()
cursor = conn.cursor()

# Set up Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='postgres-writer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to Kafka topic {KAFKA_TOPIC}...")

for message in consumer:
    try:
        data = message.value
        # Assuming data has {"temperature": 25.5, "humidity": 60.1}
        # We store the raw JSON in a payload column for flexibility
        cursor.execute(
            "INSERT INTO sensor_data (time, payload) VALUES (NOW(), %s)",
            [Json(data)]
        )
        conn.commit()
        print(f"Inserted into DB: {data}")
    except Exception as e:
        print(f"Error inserting into DB: {e}")
        conn.rollback()
