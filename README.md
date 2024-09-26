#Introduction:
#producer.py:
-This scipt will:
     +Fetch user data from the RandomUser API at https://randomuser.me.
     +Process or modify the fetched data as needed.
     +Send the data to a Kafka producer.
-Key steps:
     +Use Python's requests library to fetch data from the RandomUser API.
     +Process the data if necessary (e.g., filtering or transforming fields).
     +Connect to the Kafka broker and push the data into a Kafka topic.
#consumer.py:
-This script will:
      +Consume data from the Kafka topic where the producer sends data.
      +Save the consumed data to HDFS (Hadoop Distributed File System).
-Key steps:
      +Connect to the Kafka consumer to listen to messages from the topic.
      +Receive the messages and save them in HDFS.
      +Use Spark to write the data to HDFS in the desired format JSON.
#distributing_age.py:
-This script will:
      +Use Apache Spark to process the data stored in HDFS.
      +Transform and distribute the data based on user age groups (for example: under 18, 18-35, 35+).
      +Save the processed and grouped data back into HDFS.
-Key steps:
      +Load data from HDFS using Spark.
      +Transform the data (distribute it based on age).
      +Save the transformed data back to HDFS

    
