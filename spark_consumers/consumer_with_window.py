from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

KAFKA_TOPIC = "flights-topic"
KAFKA_BROKER = "kafka2:19093"

schema_string = "id INTEGER, legId STRING, firstSegmentDepartureTimeRaw TIMESTAMP, secondSegmentDepartureTimeRaw TIMESTAMP, "\
+ "firstSegmentArrivalTimeRaw TIMESTAMP, secondSegmentArrivalTimeRaw TIMESTAMP, "\
+ "firstSegmentArrivalAirportCode STRING, secondSegmentArrivalAirportCode STRING, "\
+ "firstSegmentDepartureAirportCode STRING, secondSegmentDepartureAirportCode STRING, "\
+ "firstSegmentAirlineName STRING, secondSegmentAirlineName STRING, "\
+ "firstSegmentAirlineCode STRING, secondSegmentAirlineCode STRING, "\
+ "firstSegmentEquipmentDescription STRING, secondSegmentEquipmentDescription STRING, "\
+ "firstSegmentDurationInSeconds INTEGER, secondSegmentDurationInSeconds INTEGER, "\
+ "firstSegmentDistance INTEGER, secondSegmentDistance INTEGER, "\
+ "firstSegmentCabinCode STRING"

def save_to_postgres(result, epoch_id, tablename):
    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/postgres").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", tablename).\
        option("user", "postgres").\
        option("password", "postgres").\
        mode("append").save()

if __name__ == '__main__':
    HDFS_NAMENODE = "hdfs://namenode:9000"

    print("Spark Streaming ...")
    spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BROKER)\
        .option("subscribe", KAFKA_TOPIC)\
        .load()
    batch_df = spark.read.csv(HDFS_NAMENODE + "/transform/batch_final.csv", header=True)
    
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_csv(col("value"), schema_string).alias("flights"))
    df = df.select("flights.*")

    df.printSchema()
    
    result = df.groupBy("firstSegmentAirlineName", window(df.firstSegmentDepartureTimeRaw, "1 day"))\
                .agg(count("*").alias("flights_count"))
    
    result = result.withColumn("window", to_csv(col("window")))
    result = result.select("window", "firstSegmentAirlineName", "flights_count")
    
    query = result.writeStream.outputMode("complete")\
        .foreachBatch(lambda df, epoch_id: save_to_postgres(df, epoch_id, "result"))\
        .start()
    query.awaitTermination()

    print("Spark Streaming finished...")