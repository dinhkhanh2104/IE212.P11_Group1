from kafka import KafkaConsumer, KafkaProducer
import yaml
import cv2
import numpy as np
from ultralytics import YOLO
import supervision as sv

def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

class TrafficViolationDetector:
    def __init__(self, config_path):
        self.config = load_kafka_config(config_path)
        self.topic = self.config['consumer']['topic']
        self.result_topic = self.config['producer1']['topic']
        self.bootstrap_server = self.config['bootstrap_server']
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_server,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda value: value
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda v: v,
            key_serializer=lambda v: v if isinstance(v, bytes) else v.encode('utf-8')
        )
        self.model = YOLO(self.config['model_path'])
        self.bounding_box_annotator = sv.BoundingBoxAnnotator()
        self.label_annotator = sv.LabelAnnotator()
        print(f"Initialized Kafka consumer for topic: {self.topic}")
        print(f"Initialized Kafka producer for topic: {self.result_topic}")

    def process_frame(self, frame_bytes):
        frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
        if frame is None:
            print("Failed to decode frame")
            return None, False

        results = self.model(frame)[0]
        detections = sv.Detections.from_ultralytics(results)

        is_green = 3 in detections.class_id
        violation_classes = [0, 6] if is_green else [6]
        violation_detected = any(cls in detections.class_id for cls in violation_classes)

        annotated_frame = self.bounding_box_annotator.annotate(scene=frame, detections=detections)
        annotated_frame = self.label_annotator.annotate(annotated_frame, detections)

        # Handle draw border to violated objects
        mask = np.isin(detections.class_id, violation_classes)
        violation_detection = detections[mask]
        # violation_detection.class_id = violation_detected
        violation_object_frame = self.bounding_box_annotator.annotate(scene=frame, detections=violation_detection)
        violation_object_frame = self.label_annotator.annotate(violation_object_frame, violation_detection)

        return annotated_frame, violation_detected, violation_object_frame

    def run(self):
        print("Starting to consume messages from Kafka...")
        for message in self.consumer:
            frame_bytes = message.value
            print("Frame received")
            annotated_frame, violation_detected, violation_object_frame = self.process_frame(frame_bytes)

            if annotated_frame is not None:
                cv2.imshow("Traffic Violation Detection", annotated_frame)

                if violation_detected:
                    print("Violation detected!")
                    self.producer.send(self.result_topic, key=message.key, value=violation_object_frame + b'|violation')

                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break

        self.consumer.close()
        self.producer.close()
        cv2.destroyAllWindows()

if __name__ == "__main__":
    config_file_path = "./configs/kafka_config.yml"
    detector = TrafficViolationDetector(config_file_path)
    detector.run()
