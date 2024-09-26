def transform():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    spark = SparkSession.builder.appName('um').getOrCreate()
    
    hdfs_path = 'hdfs://localhost:9000/hadoop/kafka/savefile/part-00000-7f1a5e70-6c32-4348-b264-fa49ae247c10-c000.json'
    
    # Đọc dữ liệu JSON
    df_json = spark.read.json(hdfs_path)
    
    # Hiển thị dữ liệu và cấu trúc để xác định chính xác
    df_json.show(truncate=False)
    df_json.printSchema()
    
    # Giả sử dữ liệu JSON được lưu dưới dạng STRUCT trong cột 'data'
    # Chuyển đổi từ STRUCT thành các cột riêng biệt
    df_corrected = df_json.select(
        col('data.address').alias('address'),
        col('data.age').cast(IntegerType()).alias('age'),
        col('data.city').alias('city'),
        col('data.country').alias('country'),
        col('data.email').alias('email'),
        col('data.first_name').alias('first_name'),
        col('data.gender').alias('gender'),
        col('data.last_name').alias('last_name'),
        col('data.phone').alias('phone'),
        col('data.registered').alias('registered'),
        col('data.username').alias('username')
    )

    # Tạo bảng tạm
    df_corrected.createOrReplaceTempView('kafka_data')
    
    # Truy vấn dữ liệu
    result = spark.sql('SELECT age, phone, gender,username FROM kafka_data WHERE age > 40 ORDER BY age ASC')
    result.show()
    
    # Lưu dữ liệu
    result.write \
        .format('json') \
        .option('path', 'hdfs://localhost:9000/hadoop/kafka/save') \
        .mode('overwrite') \
        .save()

if __name__ == "__main__":
    transform()
