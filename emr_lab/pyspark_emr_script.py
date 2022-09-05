from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, DecimalType
from pyspark.sql.functions import concat, lit, col, to_date, dayofweek,date_format, weekofyear
from pyspark.sql.functions import udf
from pyspark.sql.functions import avg, sum, count
from pyspark.sql.window import Window
import argparse
import boto3
import json


spark = SparkSession \
          .builder \
          .appName("ELR LAB") \
          .getOrCreate()


def read_csv(path):
        df=spark.read\
          .option("delimiter", ",")\
          .option("header", "true")\
          .csv(path)
        return df


def write_csv(df, output_path):
        df.write\
        .option("header", "true")\
        .mode("overwrite")\
        .parquet(output_path)


path = []
for m in range(1,13):
  path.append(f"s3://weclouddata/datasets/transformation/nyc_taxi_data/data/green_tripdata_2015-{m:02d}.csv")


if __name__ == "__main__":
	# parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_path', help="The URI for you CSV output data, like an S3 bucket location.")
    args = parser.parse_args()

    output_path = args.output_path

	# Part -1
	df=read_csv(path)
	df=df.fillna(value=0)
	df=df.na.fill("None")

	# 1) Printout the result of total number of the passengers
	total_num_rows=df.count()
	print(total_num_rows)

	# 2) Printout the avg number of the passengers per trip
	avg_pasg_num=df.select(avg(col("Passenger_count"))).collect()
	print(avg_pasg_num[0][0])

	# 3) Create a dataframe record how many total trips with 0,1,2,3,…,9 passenger`
	df_p10=df.select(col("Passenger_count")).filter(col("Passenger_count").between(1,10))
	df_p10_count=df_p10.groupBy(col("Passenger_count")).count()
	df_p10_count.show()


	#4) Create a dataframe based on the initial dataframe by:

	# removing rides with zero fare.
	# adding a new column tip_fraction that is equal to the ratio of the tip to the fare.
	# adding a new column tip_per_passenger_count with the mean of the tip_fraction per unique number of passenger_count column.


	df_4=df.filter(col("Fare_amount")!=0).withColumn("tip_fraction", (col("Tip_amount")/col("Fare_amount")))

	# In order to add column tip_per_passenger_count, we use window function
	windowPartition = Window.partitionBy("passenger_count").orderBy("passenger_count")
	df_4=df_4.withColumn("tip_per_passenger_count", avg("tip_fraction").over(windowPartition))
	df_4.select("tip_fraction", "tip_per_passenger_count").show()


	# 5) Create a dataframe based on the above dataframe grouping by the payment_type and:

	# add a column by calculating the average tip_fraction of each payment_type of each day
	# add a column by calculating the average tip_fraction of each payment_type of each hour
	df_5=df_4.withColumn("weekday", date_format(to_date(col("lpep_pickup_datetime")), "EEEE"))
	df_5.groupBy(col("payment_type")).pivot("weekday").avg("tip_fraction").show()



	# Part-2
	# Create a fact table aggregate by week per vendor id, per trip type, per payment type

	df=read_csv(path)
	df=df.fillna(value=0)
	df.show(3)

	# Add new columns "weeknum" and "Generous_customer_flg"
	df_new = df.withColumn("weeknum", weekofyear(col("lpep_pickup_datetime"))).withColumn("Generous_customer_flg", col("Tip_amount")>df.select(avg(col("Tip_amount"))).collect()[0][0])
	df_group=df_new.groupBy("weeknum","VendorID", "Trip_type ", "payment_type")
	df_week=df_group.agg(sum("Trip_distance").alias("Total_trip_distance"),\
                 avg("Trip_distance").alias("Avg_trip_distance"),\
                 sum("Fare_amount").alias("Total_Fare_amount"),\
                 avg("Fare_amount").alias("Avg_Fare_amount"),\
                 sum("Extra").alias("Total_Extra"),\
                 avg("Extra").alias("Avg_Extra"),\
                 sum("MTA_tax").alias("Total_MTA_tax"),\
                 avg("Tip_amount").alias("Avg_Tip_amount"),\
                 avg("improvement_surcharge").alias("Avg_improvement_surcharge"),\
                 count("Generous_customer_flg").alias("week_Generous_customer_flg_count"))
	df_week= df_week.withColumn("Lucky flg", col("week_Generous_customer_flg_count")>df_week.select(avg(col("week_Generous_customer_flg_count"))).collect()[0][0])


	write_csv(df_week, output_path)
