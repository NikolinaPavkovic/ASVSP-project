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
    
    df = df.selectExpr("timestamp","CAST(value AS STRING)")
    df = df.select(col("timestamp"),from_csv(col("value"), schema_string).alias("flights"))
    df = df.select("flights.*")
    
    batch_df = batch_df.filter("isNonStop == 'False'")
    batch_df = batch_df.drop("id")
    batch_df = batch_df.withColumnRenamed("legId", "flight_id")
    
    time_difference = df.withColumn('DiffInMinutes',(col("secondSegmentDepartureTimeRaw").cast("long") - col('firstSegmentArrivalTimeRaw').cast("long"))/60)

    join_tables = time_difference.join(batch_df, batch_df.flight_id == time_difference.legId, "inner")
    join_tables = join_tables.select("legId","startingAirport","destinationAirport", "firstSegmentArrivalAirportCode","firstSegmentArrivalTimeRaw", "secondSegmentDepartureTimeRaw", "DiffInMinutes")
    
    join_tables.printSchema()
    
    query = join_tables.writeStream.outputMode("update")\
        .foreachBatch(lambda df, epoch_id: save_to_postgres(df, epoch_id, "join_tables"))\
        .start()
    query.awaitTermination()

    print("Spark Streaming finished...")