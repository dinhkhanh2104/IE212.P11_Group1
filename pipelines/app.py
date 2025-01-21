from kafka import KafkaConsumer
import cv2
import numpy as np
import base64
from pymongo import MongoClient
import yaml

def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Load Kafka configuration
config_file_path = "./configs/kafka_config.yml"
config = load_kafka_config(config_file_path)

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['traffic_violation']
collection = db['violations']

def save_to_mongodb(frame, frame_id):
    _, buffer = cv2.imencode('.jpg', frame)
    frame_base64 = base64.b64encode(buffer).decode('utf-8')
    collection.insert_one({
        'frame_id': frame_id,
        'frame': frame_base64
    })

# Initialize Kafka consumer
consumer = KafkaConsumer(
    config['consumer1']['topic'],
    bootstrap_servers=config['bootstrap_server'],
    auto_offset_reset=config['consumer1']['auto_offset_reset'],
    enable_auto_commit=config['consumer1']['enable_auto_commit'],
    value_deserializer=lambda x: x,
    key_deserializer=lambda x: x
)

# Display the frames
cnt = -1
try:
    for message in consumer:
        frame_bytes = message.value
        
        if b'|violation' in frame_bytes:
            cnt += 1
            frame_bytes, _ = frame_bytes.split(b'|violation')
            frame_id = message.key.decode('utf-8')
            frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            
            if frame is not None:
                if cnt % 10 == 0:
                    try:
                        save_to_mongodb(frame, frame_id)
                    except Exception as e:
                        print(f"Error saving to MongoDB: {e}")
                cv2.imshow('Processed Video', frame)
                
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
            else:
                print("No frame")
        else:
            frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            if frame is not None:
                cv2.imshow('Processed Video', frame)
                
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
            else:
                print("No frame")
except Exception as e:
    print(f"Error while consuming messages: {e}")
finally:
    cv2.destroyAllWindows()
    consumer.close()
