from pyspark.sql.session import SparkSession

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"

    spark = SparkSession.builder.appName("HDFSData").getOrCreate()

    df = spark.read.csv("../spark/producers/batch/data/batch_final.csv", header = True)
    df.write.csv(HDFS_NAMENODE + "/raw/batch_final.csv", mode="overwrite", header=True)