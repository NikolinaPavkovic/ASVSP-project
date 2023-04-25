from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg,min,max,rank,col, desc, count, when, sum, to_date, date_format, row_number

from pyspark.sql.types import IntegerType, DoubleType, BooleanType

HDFS_NAMENODE = "hdfs://namenode:9000"

spark = SparkSession.builder.appName("HDFSDataCurated").getOrCreate()

# Prosecna cena karte za svaku rutu (pocetni i odredisni aerodrom)
def avg_basefare(df):
    window = Window.partitionBy("startingAirport", "destinationAirport")
    avgBaseFare = avg("baseFare").over(window).alias('avgBaseFare')
    result = df.select("startingAirport", "destinationAirport", avgBaseFare).orderBy("startingAirport").distinct()
    save_to_postgres(result, "avg_basefare")


# Minimalno, maksimalno i prosecno trajanje putovanja za svaku rutu (pocetni i odredisni aerodrom)
def min_max_avg_travelDuration(df):

    window = Window.partitionBy("startingAirport", "destinationAirport")

    result = df.select("startingAirport", "destinationAirport", 
                       min("travelDuration").over(window).alias("minTravelDuration"), 
                       max("travelDuration").over(window).alias("maxTravelDuration"), 
                       avg("travelDuration").over(window).alias("avgTravelDuration")).distinct()
    save_to_postgres(result, "min_max_avg_travelDuration")


# Razlika u ceni karata izmedju svakog leta i prosecne cene karata za rutu
def diff_totalFare(df):
    window = Window.partitionBy("startingAirport", "destinationAirport")

    result = df.withColumn("fareDifference", col("totalFare") - avg("totalFare")
                           .over(window)).select("legId", "startingAirport", "destinationAirport", "totalFare", "fareDifference")
    save_to_postgres(result, "diff_totalFare")


# Ukupna zarada po ruti za sve letove
# sortirano po datumu leta
def cumulative_total_fare(df):
    window_spec = Window.partitionBy("startingAirport", "destinationAirport").orderBy("flightDate")

    result = df.select("startingAirport", "destinationAirport", "flightDate",
                        sum("totalFare").over(window_spec).alias("cumTotalFare")).orderBy("flightDate")
    save_to_postgres(result, "cumulative_total_fare")


# Procenat letova bez presedanja za svaki pocetni aerodrom, i rangiranje
# svake grupe aerodroma po procentu letove bez presedanja
def non_stop_percentage(df):
    window = Window.orderBy(desc("nonStopPercentage"))

    result = df.withColumn("isNonStop", when(col("isNonStop") == 1, 1).otherwise(0))\
            .groupBy("startingAirport")\
            .agg(sum("isNonStop") / count("*") * 100.0)\
            .withColumnRenamed("((sum(isNonStop) / count(1)) * 100.0)", "nonStopPercentage")\
            .withColumn("airportRank", rank().over(window))\
            .select("startingAirport", "nonStopPercentage", "airportRank")

    save_to_postgres(result, "non_stop_percentage")


# Minimalna i maksimalna udaljenost za svaku rutu (pocetni i odredisni aerodrom)
def min_max_travelDistance(df):

    window = Window.partitionBy("startingAirport", "destinationAirport")

    result = df.select("startingAirport", "destinationAirport", 'totalTravelDistance',
                       min("totalTravelDistance").over(window).alias("minTravelDistance"), 
                       max("totalTravelDistance").over(window).alias("maxTravelDistance"))
    save_to_postgres(result, "min_max_travelDistance")

#7
def biggest_total_fair_by_month_for_flight(df):

    window = Window.partitionBy(date_format('flightDate', 'yyyy-MM'))

    result = df.select('legId', 'flightDate', 'startingAirport', 'destinationAirport', 'totalFare',
                            row_number().over(window.orderBy(col('totalFare').desc())).alias('fareRank'))

    result.show()

    save_to_postgres(result, "remaining_seats_percentage")

#8
def airport_income_by_month(df):
    window = Window.partitionBy('startingAirport').orderBy('flightDate', 'totalFare', 'legId')

    df = df.withColumn('month', date_format('flightDate', 'yyyy-MM'))
    df = df.withColumn('cumulative_income', sum('totalFare').over(window))
    df = df.select('legId', 'startingAirport', 'month', 'totalFare', 'cumulative_income')

    df.show()
    save_to_postgres(df, "airport_income_by_month")

#9
def income_of_starting_airport_by_month_cum(df):
    window = Window.partitionBy('startingAirport').orderBy('month', 'total_income')
    
    df = df.withColumn('month', date_format('flightDate', 'yyyy-MM')) \
    .groupBy('startingAirport', 'month') \
    .agg(sum('totalFare').alias('total_income'))\
    .orderBy('startingAirport', 'month') \
    
    df = df.withColumn('cumulative_income', sum('total_income').over(window))
    df.show()

    save_to_postgres(df, "income_of_starting_airport_by_month_cum")





def running_avg_totalfare(df):

    window = Window.partitionBy("startingAirport")
    window1 = Window.orderBy("cumAvgTotalFare")

    result = df.withColumn("cumAvgTotalFare", avg("totalFare").over(window))\
            .withColumn("airportRank", rank().over(window1))\
            .select("startingAirport","cumAvgTotalFare", "airportRank")
    
    save_to_postgres(result, "running_avg_totalfare")




def save_to_postgres(result, tablename):
    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/postgres").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", tablename).\
        option("user", "postgres").\
        option("password", "postgres").\
        mode("overwrite").save()


if __name__ == '__main__':
    # load dataframe
    df = spark.read.csv(HDFS_NAMENODE + "/transform/batch_final.csv", header=True)
    df = df.withColumn("travelDuration", col("travelDuration").cast(IntegerType()))
    df = df.withColumn("isNonStop", col("isNonStop").cast(BooleanType()))
    df = df.withColumn("elapsedDays", col("elapsedDays").cast(IntegerType()))
    df = df.withColumn("baseFare", col("baseFare").cast(DoubleType()))
    df = df.withColumn("totalFare", col("totalFare").cast(DoubleType()))
    df = df.withColumn("seatsRemaining", col("seatsRemaining").cast(IntegerType()))
    df = df.withColumn("totalTravelDistance", col("totalTravelDistance").cast(IntegerType()))

    #df.show()

    #call queries

    #avg_basefare(df)
    #min_max_avg_travelDuration(df)
    #diff_totalFare(df)
    #non_stop_percentage(df)
    #cumulative_total_fare(df)
    #min_max_travelDistance(df)
    #biggest_total_fair_by_month_for_flight(df)
    #airport_income_by_month(df)
    #income_of_starting_airport_by_month_cum(df)
    
    
    
    #running_avg_totalfare(df)