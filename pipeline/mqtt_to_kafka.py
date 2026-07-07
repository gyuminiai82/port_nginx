import os  # 운영체제 환경변수를 읽어오기 위한 모듈
import json  # 데이터를 JSON 형식으로 변환하거나 파싱하기 위한 모듈
import time  # 코드 실행을 잠시 멈추는 등 시간 관련 기능을 위한 모듈
import paho.mqtt.client as mqtt  # MQTT 브로커(EMQX)와 통신하기 위한 paho 라이브러리
from kafka import KafkaProducer  # Kafka 브로커로 데이터를 보내기 위한 프로듀서 라이브러리

# --- [EMQX (MQTT) 접속 정보 설정] ---
# 환경변수에서 값을 가져오되, 없으면 기본값("port_emqx")을 사용합니다.
MQTT_BROKER = os.getenv("MQTT_BROKER", "port_emqx")  # EMQX 도커 컨테이너 이름 (주소 역할)
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))  # EMQX 기본 통신 포트 번호
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensor/data")  # 구독할 MQTT 주제(Topic) - Node-RED가 보내는 주소

# --- [Kafka 접속 정보 설정] ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "port_kafka:9092")  # Kafka 도커 컨테이너 이름 및 포트
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")  # 데이터를 밀어넣을 Kafka 주제(Topic)

# 도커 컨테이너들이 모두 켜질 때까지 충분히 기다려줍니다 (10초 대기)
time.sleep(10)

# --- [Kafka 프로듀서 (발행자) 생성] ---
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],  # 연결할 Kafka 브로커 주소
    # 파이썬 딕셔너리 데이터를 JSON 문자열로 바꾼 뒤, UTF-8 바이트(Byte) 형태로 변환해서 전송하는 설정
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- [MQTT 연결 성공 시 실행되는 콜백 함수] ---
def on_connect(client, userdata, flags, rc):
    # rc(Result Code)가 0이면 정상 연결됨을 의미합니다.
    print(f"Connected to MQTT broker with result code {rc}")
    # 연결 성공 후, 우리가 원하는 주제("sensor/data")를 구독(Subscribe) 신청합니다.
    client.subscribe(MQTT_TOPIC)

# --- [MQTT 브로커로부터 새 메시지가 도착할 때마다 실행되는 콜백 함수] ---
def on_message(client, userdata, msg):
    try:
        # 수신된 바이트(Byte) 데이터를 사람이 읽을 수 있는 문자열(UTF-8)로 변환합니다.
        payload_str = msg.payload.decode("utf-8")
        # 문자열을 파이썬에서 다루기 쉬운 딕셔너리(JSON 객체) 형태로 파싱합니다.
        data = json.loads(payload_str)
        print(f"Received from MQTT: {data}")  # 터미널에 수신된 데이터 출력
        
        # 가공된 데이터를 Kafka 브로커의 "sensor_data" 토픽으로 전송(Publish)합니다.
        producer.send(KAFKA_TOPIC, data)
        # 즉시 전송되도록 버퍼를 비워줍니다 (실시간성 확보).
        producer.flush()
    except Exception as e:
        # 데이터 처리 중 에러가 발생하면 로그를 출력하고 프로그램이 죽지 않게 방어합니다.
        print(f"Error processing message: {e}")

# --- [MQTT 클라이언트(구독자) 객체 생성 및 세팅] ---
client = mqtt.Client()  # MQTT 클라이언트 로봇 생성
client.on_connect = on_connect  # "연결되면 on_connect 함수를 실행해라" 라고 지정
client.on_message = on_message  # "메시지가 오면 on_message 함수를 실행해라" 라고 지정

# EMQX 브로커가 완전히 켜질 때까지 무한 반복하며 연결을 시도합니다.
while True:
    try:
        # EMQX 브로커에 연결을 시도합니다 (호스트, 포트, 연결 유지 시간 60초 설정).
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        break  # 연결에 성공하면 무한 루프를 탈출합니다.
    except Exception as e:
        # 연결에 실패하면 5초 쉬었다가 다시 시도합니다.
        print(f"Waiting for MQTT broker... {e}")
        time.sleep(5)

# 스크립트가 종료되지 않고 24시간 내내 돌면서 메시지를 기다리게 만듭니다 (무한 루프).
client.loop_forever()
