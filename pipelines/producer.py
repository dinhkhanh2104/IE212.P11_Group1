from kafka import KafkaProducer
import yaml
import cv2
import time
import os

def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

class TrafficViolationProducer:
    def __init__(self, config_path):
        self.config = load_kafka_config(config_path)
        self.topic = self.config['producer']['topic']
        self.producer = KafkaProducer(
            bootstrap_servers=self.config['bootstrap_server'],
            key_serializer=lambda key: key.encode('utf-8'),
            value_serializer=lambda value: value
        )
    
    def send_frame(self, frame, key):
        _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 50])
        self.producer.send(self.topic, key=key, value=buffer.tobytes())
        self.producer.flush()

    def close(self):
        self.producer.close()

def find_input_video(base_path):
    for ext in ['.MOV', '.mp4']:
        video_path = base_path + ext
        if os.path.exists(video_path):
            return video_path
    return None

if __name__ == '__main__':
    config_file_path = "./configs/kafka_config.yml"
    base_video_path = "../input"

    video_path = find_input_video(base_video_path)
    if video_path is None:
        print("Error: No video file found")
        exit()

    producer = TrafficViolationProducer(config_file_path)

    video = cv2.VideoCapture(video_path)
    if not video.isOpened():
        print("Error: Cannot open video file")
        exit()

    while True:
        success, frame = video.read()
        if not success:
            print("End of video or failed to read frame")
            break

        frame = cv2.resize(frame, (720, 720))
        
        # Send frame to Kafka
        producer.send_frame(frame, "frame")
 
        time.sleep(0.001)
        
        # Exit if 'q' is pressed
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    video.release()
    cv2.destroyAllWindows()
    producer.close()
