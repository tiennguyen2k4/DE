def data_from_kafka_to_hadoop():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col,from_json
    from pyspark.sql.types import StringType,IntegerType,StructField,StructType
    
    spark=SparkSession.builder.appName('kafka_hadoop').getOrCreate()
    
    kafka_data=spark.readStream \
        .format('kafka') \
            .option('kafka.bootstrap.servers','localhost:9092') \
                .option('subscribe','usekafka2') \
                    .option('startingOffsets','earliest') \
                        .load()
                        
    json_schema = StructType([
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('country', StringType(), True),
    StructField('city', StringType(), True),
    StructField('address', StringType(), True),
    StructField('email', StringType(), True),
    StructField('username', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('registered', StringType(), True),
    StructField('phone', StringType(), True)
    ])
    #chuyển các dữ liệu nhị phân trong kafka sang string
    kafka_df_string=kafka_data.selectExpr("CAST(value AS STRING) as json_string")
    #chuyển json sang datafame
    json_df=kafka_df_string.select(from_json(col('json_string'),json_schema).alias('data'))
    #lưu vào hdfs
    save_data=json_df.writeStream \
        .format('json') \
            .option('path','hdfs://localhost:9000/hadoop/kafka/savefile') \
                .option('checkpointLocation','hdfs://localhost:9000/hadoop/kafka/check') \
                    .outputMode('append') \
                        .start()
    #khởi động                   
    save_data.awaitTermination()

    

if __name__ == "__main__":
    data_from_kafka_to_hadoop()