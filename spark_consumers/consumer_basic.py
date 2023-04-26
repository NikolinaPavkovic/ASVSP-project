from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

KAFKA_TOPIC = "flights-topic"
KAFKA_BROKER = "kafka2:19093"

schema = StructType()\
    .add("id", IntegerType())\
    .add("legId", StringType())\
    .add("firstSegmentDepartureTimeRaw", DateType())\
    .add("secondSegmentDepartureTimeRaw", DateType())\
    .add("firstSegmentArrivalTimeRaw", DateType())\
    .add("secondSegmentArrivalTimeRaw", DateType())\
    .add("firstSegmentArrivalAirportCode", StringType())\
    .add("secondSegmentArrivalAirportCode", StringType())\
    .add("firstSegmentDepartureAirportCode", StringType())\
    .add("secondSegmentDepartureAirportCode", StringType())\
    .add("firstSegmentAirlineName", StringType())\
    .add("secondSegmentAirlineName", StringType())\
    .add("firstSegmentAirlineCode", StringType())\
    .add("secondSegmentAirlineCode", StringType())\
    .add("firstSegmentEquipmentDescription", StringType())\
    .add("secondSegmentEquipmentDescription", StringType())\
    .add("firstSegmentDurationInSeconds", IntegerType())\
    .add("secondSegmentDurationInSeconds", IntegerType())\
    .add("firstSegmentDistance", IntegerType())\
    .add("secondSegmentDistance", IntegerType())\
    .add("firstSegmentCabinCode", StringType())

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
    print("Spark Streaming ...")
    spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BROKER)\
        .option("subscribe", KAFKA_TOPIC)\
        .load()
    
    df = df.selectExpr("timestamp","CAST(value AS STRING)")
    df = df.select(col("timestamp"),from_csv(col("value"), schema_string).alias("flights"))
    df = df.select("flights.*")
    #df.printSchema()

    df = df.filter("firstSegmentEquipmentDescription != 'NULL' ")

    data_for_each_airplane_type = df.groupBy("firstSegmentAirlineName", "firstSegmentEquipmentDescription")\
                                    .agg(count("firstSegmentEquipmentDescription").alias("num_of_flights"),
                                         (sum("firstSegmentDurationInSeconds")/60).alias("total_time_flying"),
                                         sum("firstSegmentDistance").alias("total_distance"))
    
    query = data_for_each_airplane_type.writeStream.outputMode("update")\
        .foreachBatch(lambda df, epoch_id: save_to_postgres(df, epoch_id, "data_for_each_airplane_type"))\
        .start()
    query.awaitTermination()

    print("Spark Streaming finished...")