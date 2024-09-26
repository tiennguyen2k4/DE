# Introduction
This project utilizes Kafka for data streaming, HDFS for distributed storage, and Spark for data processing. The main components are outlined below:

## producer.py
- This script will:
  - Fetch user data from the RandomUser API at [https://randomuser.me](https://randomuser.me).
  - Process or modify the fetched data as needed.
  - Send the data to a Kafka producer.

### Key Steps:
  - Use Python's `requests` library to fetch data from the RandomUser API.
  - Process the data if necessary (e.g., filtering or transforming fields).
  - Connect to the Kafka broker and push the data into a Kafka topic.

---

## consumer.py
- This script will:
  - Consume data from the Kafka topic where the producer sends data.
  - Save the consumed data to HDFS (Hadoop Distributed File System).

### Key Steps:
  - Connect to the Kafka consumer to listen to messages from the topic.
  - Receive the messages and save them in HDFS.
  - Use Spark to write the data to HDFS in the desired format (e.g., JSON).

---

## distributing_age.py
- This script will:
  - Use Apache Spark to process the data stored in HDFS.
  - Transform and distribute the data based on user age groups (for example: under 18, 18-35, 35+).
  - Save the processed and grouped data back into HDFS.

---

