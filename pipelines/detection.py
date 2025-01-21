import findspark
from utils import load_kafka_config
import numpy as np
from ultralytics import YOLO
import cv2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
import supervision as sv
import os

class TrafficViolationDetector:
    def __init__(self, config_path):
        self.config = load_kafka_config(config_path)
        self.topic = self.config['consumer']['topic']
        self.result_topic = self.config['producer1']['topic']
        self.bootstrap_server = self.config['bootstrap_server']
        self.spark = SparkSession.builder \
            .appName("TrafficViolationDetection") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()
        self.model = YOLO(self.config['model_path'])
        self.bounding_box_annotator = sv.BoundingBoxAnnotator()
        self.label_annotator = sv.LabelAnnotator()
        print(f"Initialized Spark session for topic: {self.topic}")

    def process_frame_udf(self, frame_bytes):
        frame = np.frombuffer(frame_bytes, dtype=np.uint8)
        frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
        if frame is None:
            return []

        results = self.model(frame)[0]
        detections = sv.Detections.from_ultralytics(results)

        # Check if green is in class_id
        is_green = 3 in detections.class_id or 7 not in detections.class_id or 8 not in detections.class_id

        # Determine violation classes
        if is_green:
            violation_detections = [class_id for class_id in detections.class_id if class_id == 0 or class_id == 6]
        else:
            violation_detections = [class_id for class_id in detections.class_id if class_id == 6]

        annotated_frame = self.bounding_box_annotator.annotate(scene=frame, detections=detections)
        annotated_frame = self.label_annotator.annotate(annotated_frame, detections)

        _, buffer = cv2.imencode('.jpg', annotated_frame)
        frame_bytes = buffer.tobytes()

        # Add a flag to indicate if violation is detected
        if violation_detections:
            frame_bytes += b'|violation'

        return frame_bytes

    def run(self):
        process_udf = udf(self.process_frame_udf, BinaryType())

        spark_df = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()

        spark_df = spark_df.withColumn("value", process_udf("value"))

        checkpoint_location = os.path.join(os.path.dirname(__file__), '../logs/checkpoint1')
        query = spark_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("topic", self.result_topic) \
            .option("checkpointLocation", checkpoint_location) \
            .start()

        query.awaitTermination()
        query.stop()
        self.spark.stop()

if __name__ == "__main__":
    config_file_path = "./configs/kafka_config.yml"
    detector = TrafficViolationDetector(config_file_path)
    detector.run()
