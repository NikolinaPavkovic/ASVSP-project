from pyspark.sql.session import SparkSession

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"

    spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

    df = spark.read.csv(HDFS_NAMENODE + "/raw/batch_final.csv", mode="overwrite")
    df = df.na.drop()
    df = df.drop("segmentsDepartureTimeEpochSeconds")
    df = df.drop("segmentsArrivalTimeEpochSeconds")

    df.write.csv(HDFS_NAMENODE + "/transform/batch_final.csv", mode="overwrite", header=True)
