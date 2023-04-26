from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"

    spark = SparkSession.builder.appName("HDFSDataRaw").getOrCreate()

    df = spark.read.csv("../spark/producers/batch/data/batch_final.csv", header=True)

    df.show()

    df.write.csv(HDFS_NAMENODE + "/raw/batch_final.csv", mode="overwrite", header=True)