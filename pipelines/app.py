from kafka import KafkaConsumer
import cv2
import numpy as np
import time
import base64
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['traffic_violation']
collection = db['violations']
collection.delete_many({})
print("Connected to MongoDB successfully")    

def save_to_mongodb(frame, frame_id):
    _, buffer = cv2.imencode('.jpg', frame)
    frame_base64 = base64.b64encode(buffer).decode('utf-8')
    collection.insert_one({
        'frame_id': frame_id,
        'frame': frame_base64
    })
    print('Saved frame to MongoDB')

try:
    consumer = KafkaConsumer(
        'result',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: x,
        key_deserializer=lambda x: x
    )
    print("Kafka consumer initialized successfully")
    print(f"Subscribed to topic: {consumer.subscription()}")
except Exception as e:
    print(f"Failed to initialize Kafka consumer: {e}")
    exit(1)

cnt = -1
try:
    for message in consumer:
        frame_bytes = message.value
        print("Frame received")
        if b'|violation' in frame_bytes:
            cnt += 1
            frame_bytes, _ = frame_bytes.split(b'|violation')
            frame_id = message.key.decode('utf-8')
            frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            print("Violation detected")
            if frame is not None:
                if cnt % 10 == 0:
                    try:
                        save_to_mongodb(frame, frame_id)
                    except Exception as e:
                        print(e)
                # # cv2.imshow('Processed Video', frame)
                # time.sleep(0.05)
                # if cv2.waitKey(1) & 0xFF == ord('q'):
                #     break
            else:
                print("No frame")
        else:
            frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            if frame is not None:
                ...
                # cv2.imshow('Processed Video', frame)
                # time.sleep(0.05)
                # if cv2.waitKey(1) & 0xFF == ord('q'):
                #     break
            else:
                print("No frame")
except Exception as e:
    print(f"Error while consuming messages: {e}")
finally:
    cv2.destroyAllWindows()
    consumer.close()
    print("Kafka consumer closed")
