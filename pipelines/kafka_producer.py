from kafka import KafkaProducer
import yaml
import cv2
import time
import os

def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        kafka_config = yaml.safe_load(file)
    return kafka_config

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
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]
        _, buffer = cv2.imencode('.jpg', frame, encode_param)
        self.producer.send(self.topic, key=key, value=buffer.tobytes())
        self.producer.flush()

    def close(self):
        self.producer.close()

def handle_find_input_video(base_path):
    extensions = ['.MOV', '.mp4']
    for ext in extensions:
        video_path = base_path + ext
        if os.path.exists(video_path):
            return video_path
    return None

if __name__ == '__main__':
    config_file_path = "./configs/kafka_config.yml"
    base_video_path = "../input"

    video_path = handle_find_input_video(base_video_path)
    if video_path is None:
        print(f"Error: No video file found")
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
        
        # Hiển thị frame
        # cv2.imshow("Video Frame", frame)
        
        # Gửi frame qua Kafka
        producer.send_frame(frame, "frame")
 
        time.sleep(0.001)
        
        # Thoát khi nhấn phím 'q'
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    video.release()
    cv2.destroyAllWindows()
