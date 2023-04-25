from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, split, when, regexp_replace, trim
from pyspark.sql.types import IntegerType, DoubleType

if __name__ == "__main__":

    HDFS_NAMENODE = "hdfs://namenode:9000"

    spark = SparkSession.builder.appName("HDFSDataTransform").getOrCreate()

    df = spark.read.csv(HDFS_NAMENODE + "/raw/batch_final.csv", mode="overwrite", header=True)
    df = df.drop("searchDate")
    df = df.drop("segmentsDepartureTimeEpochSeconds")
    df = df.drop("segmentsArrivalTimeEpochSeconds")

    # P{days}T{hours}H{minutes}M
    df = df.withColumn("days", split(col("travelDuration"), "T")[0].cast("string").substr(2,1))
    df = df.withColumn("hours", split(split(col("travelDuration"), "T")[1], "H")[0].cast("integer"))
    df = df.withColumn("minutes", regexp_replace(split(split(col("travelDuration"), "T")[1], "H")[1], "M", "").cast("integer"))


    df = df.withColumn("days", when(col("days") == "", 0).otherwise(col("days")))
    df = df.withColumn("hours", when(col("hours") == "", 0).otherwise(col("hours")))
    df = df.withColumn("minutes", when(col("minutes") == "", 0).otherwise(col("minutes")))

    df = df.withColumn("days", col("days").cast(IntegerType()))
    df = df.withColumn("hours", col("hours").cast(IntegerType()))
    df = df.withColumn("minutes", col("minutes").cast(IntegerType()))

    df = df.withColumn("travelDuration", col("days") * 1440 + col("hours") * 60 + col("minutes"))
    df = df.withColumn("travelDuration", col("travelDuration").cast(IntegerType()))
    df = df.withColumn("elapsedDays", col("elapsedDays").cast(IntegerType()))
    df = df.withColumn("baseFare", col("baseFare").cast(DoubleType()))
    df = df.withColumn("totalFare", col("totalFare").cast(DoubleType()))
    df = df.withColumn("seatsRemaining", col("seatsRemaining").cast(IntegerType()))
    df = df.withColumn("totalTravelDistance", col("totalTravelDistance").cast(DoubleType()))

    df = df.drop("days")
    df = df.drop("hours")
    df = df.drop("minutes")

    df = df.na.drop()

    for col_name, data_type in df.dtypes:
        print(col_name, data_type)

    df.show()

    df.write.csv(HDFS_NAMENODE + "/transform/batch_final.csv", mode="overwrite", header=True)
