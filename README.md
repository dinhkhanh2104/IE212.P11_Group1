# IE212.P11_Group1_Traffic_Violation_Detection_System

## Description

This project is designed to detect traffic violations by processing video feeds using the YOLOv10 model for object detection. It utilizes Spark and Kafka to identify violations in real-time from video streams.

## Getting Started

### Prerequisites

Ensure you have the following installed:

- Apache Kafka
- Apache Spark
- MongoDB
- Python 3.8+
- JDK 11
- OpenCV

### Installing

1. Clone the repository:

   ```sh
   git clone https://github.com/dinhkhanh2104/IE212.P11_Group1.git
   ```

2. Create and activate a virtual environment:

   ```sh
   python -m venv myenv
   myenv\Scripts\activate  # On MacOS, use `source myenv/bin/activate`
   ```

3. Install the required dependencies:

   ```sh
   pip install -r requirements.txt
   ```

4. Start Kafka and create the necessary topics:
   - Run the batch script:
     ```sh
     scripts/setup_kafka.bat <your absolute path to kafka installation directory>
     ```
   - Or follow instructions from "setup_kafka_instruction.txt" to start Kafka manually.

### Executing Program
Before running the commands, ensure that your virtual environment is activated.

1. **Run the video stream producer**:

   ```sh
   python ./pipelines/producer.py
   ```

2. **Run the Spark consumer (read Kafka-stream and make predictions)**:

   ```sh
   python ./pipelines/detection.py
   ```

3. **Run the main consumer (display predicted images and save to MongoDB)**:

   ```sh
   python ./pipelines/consumer.py
   ```

## Common Issues

- **Cannot open video file**: Ensure the video file path is correct and the file exists.
- **Kafka server not running**: Try deleting the folder "C:/temp" and restarting Kafka.

## Authors (Group 1)

- **Pham Thanh Duy** [ptduy2603](https://github.com/ptduy2603)
- **Tran Dinh Khanh** [dinhkhanh2104](https://github.com/dinhkhanh2104)
- **Truong Thien Loc** [truongthienloc](https://github.com/truongthienloc)
