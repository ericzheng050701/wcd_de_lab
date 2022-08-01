
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col
from pyspark.sql.functions import when
import argparse




def spark_init(spark_name):
        spark = SparkSession \
        .builder \
        .appName(spark_name) \
        .getOrCreate()
        return spark

def read_csv(spark, input_file_url):
        df=spark.read\
          .option("delimiter", ",")\
          .option("header", "true")\
          .csv(input_file_url)
        return df

def write(df, output_file_url):
        df.write\
        .option("header", "false")\
        .mode("overwrite")\
        .csv(output_file_url)


if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument('--spark_name', help="spark_name")
        parser.add_argument('--input_file_url', help="input file S3 bucket location.")
        parser.add_argument('--output_file_url', help="output file S3 bucket location.")
        parser.add_argument('--avg_order_amount', help="avg_order_amount")

        args = parser.parse_args()


        spark_name = args.spark_name
        input_file_url = args.input_file_url
        output_file_url = args.output_file_url
        avg_order_amount = float(args.avg_order_amount)

        spark=spark_init(spark_name)
        df=read_csv(spark, input_file_url)
        df=df.withColumn('big_order', \
                when(df['order_amount']-avg_order_amount>0, True)
                .otherwise(False)
                )

        df.show()
        write(df, output_file_url)




## command to run the script in local:
##spark-submit --master local --deploy-mode client spark.py --spark_name 'airflow_lab' --input_file_url './data/orders_amount.csv' --output_file_url './data/orders_amount_output' --avg_order_amount '29171.860335'#
