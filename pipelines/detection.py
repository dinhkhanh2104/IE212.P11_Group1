from kafka import KafkaConsumer
import yaml
import cv2
import numpy as np
from ultralytics import YOLOv10
import supervision as sv


def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        kafka_config = yaml.safe_load(file)
    return kafka_config


class TrafficViolationDetector:
    def __init__(self, config_path):
        self.config = load_kafka_config(config_path)
        self.topic = self.config['consumer']['topic']
        self.bootstrap_server = self.config['bootstrap_server']
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_server,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda value: value
        )
        self.model = YOLOv10(self.config['model_path'])
        self.bounding_box_annotator = sv.BoundingBoxAnnotator()
        self.label_annotator = sv.LabelAnnotator()

    def process_frame(self, frame_bytes):
        # Decode the frame
        frame = np.frombuffer(frame_bytes, dtype=np.uint8)
        frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
        if frame is None:
            return None

        # Run YOLO detection
        results = self.model(frame)[0]
        detections = sv.Detections.from_ultralytics(results)

        # Determine violations
        class_ids = detections.class_id
        is_green = 3 in class_ids  # GREEN signal class
        violation_detected = False

        if is_green:
            violation_classes = [0, 6]  # BLOW THE RED LIGHT, SPEEDING
        else:
            violation_classes = [6]

        if any(cls in class_ids for cls in violation_classes):
            violation_detected = True

        # Annotate the frame
        annotated_frame = self.bounding_box_annotator.annotate(scene=frame, detections=detections)
        annotated_frame = self.label_annotator.annotate(annotated_frame, detections)

        return annotated_frame, violation_detected

    def run(self):
        for message in self.consumer:
            frame_bytes = message.value
            annotated_frame, violation_detected = self.process_frame(frame_bytes)

            if annotated_frame is not None:
                # Display the frame
                cv2.imshow("Traffic Violation Detection", annotated_frame)

                # Add violation status
                if violation_detected:
                    print("Violation detected!")

                # Exit if 'q' is pressed
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break

        # Cleanup
        self.consumer.close()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    config_file_path = "./configs/kafka_config.yml"
    detector = TrafficViolationDetector(config_file_path)
    detector.run()
