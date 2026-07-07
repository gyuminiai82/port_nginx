import os
import json
import time
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_BROKER = os.getenv("MQTT_BROKER", "port_emqx")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensor/data")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "port_kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")

# Wait for Kafka and MQTT to be ready
time.sleep(10)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)
        print(f"Received from MQTT: {data}")
        # Send to Kafka
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
    except Exception as e:
        print(f"Error processing message: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        break
    except Exception as e:
        print(f"Waiting for MQTT broker... {e}")
        time.sleep(5)

client.loop_forever()
