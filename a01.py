import time
import psycopg2
import psycopg2.extras
import pandas as pd
import sys
sys.path.append("..")
import config
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, split
from pyspark.sql.functions import col
# import time
from sqlalchemy import create_engine
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType

from pyspark.sql.functions import split
from pyspark.sql.utils import AnalysisException

# Set environment flags
flag = True
limit = True
aeroflag = False
global count
count = 0

# Store the start time when the script starts
start_time = time.time()

def get_time_elapsed():
    # Get the current time
    current_time = time.time()
    # Calculate the time elapsed from start time
    elapsed_time = current_time - start_time
    # Convert to seconds (if necessary, format or use in a more readable way)
    return elapsed_time

# Function to track and print elapsed time
def log_time(previous_time, label=""):
    current_time = time.time()
    elapsed_time = current_time - previous_time
    print(f"{label} - Time taken: {elapsed_time:.2f} seconds")
    return current_time

# Get command-line argument
def get_arg_value(arg):
    try:
        idx = sys.argv.index(arg)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return None

# Generate custom date format (YYYYMMDDHH05)
def generate_custom_datetime_format():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d%H") + "05"

# Retrieve environment and batch date from arguments
environment_var = get_arg_value("-e")
if environment_var not in {"dev", "prod", "stg", "ldev"}:
    print("Pass parameter -e as dev, prod, stg, or ldev")
    sys.exit(1)

environment = environment_var
now = datetime.now() - timedelta(days=1)
year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')

batchsize = (get_arg_value("-batchsize") or 2000)
# batchsize = get_arg_value("-batchsize") or 1000
repart = int(get_arg_value("-repart") or 150)
now_var = get_arg_value("-d")
filename = get_arg_value("-f")
if now_var:
    year, month, day = now_var[:4], now_var[4:6], now_var[6:]
    print(f"Processing batch for date: {year}-{month}-{day}")
else:
    print(f"Processing batch for date: {now}")

# Set HDFS path based on environment
if environment == "prod":
    hdfs_path = f"hdfs://172.22.115.12:8020/hhgraph/lgipmap/MergedData/consol_lat_long_interests/year=2025/month=01/day=13"
elif environment == "dev":
    hdfs_path = "hdfs://172.22.137.155:8020/temp/copyWorkflow/LG_IP/MergedData/ConsolLatLongInterests/year=2024/month=12/day=18/"

    if filename : hdfs_path = f"hdfs://172.22.137.155:8020/temp/copyWorkflow/LG_IP/MergedData/ConsolLatLongInterests/year=2024/month=12/day=18/{filename}"
elif environment == "stg":
    hdfs_path = "hdfs://10.5.53.147:8020/temp/copyWorkflow/LG_IP/MergedData/ConsolLatLongInterests/"
    if filename : hdfs_path = f"hdfs://10.5.53.147:8020/temp/copyWorkflow/LG_IP/MergedData/ConsolLatLongInterests/{filename}"
else:
    hdfs_path = "adfalcon_logs+9+105511616398+105511740301.parquet"
    # hdfs_path = "adfalcon_logs+0+81292544156+81292581915.parquet"


spark_builder = SparkSession.builder.appName('Adfalcon')

# Initialize Spark session with configurations
spark = (
    SparkSession.builder
        .config("spark.executor.memory", "64g") 
        .config("spark.driver.memory", "64g") 
        .config("spark.executor.cores", "1") 
        .config("spark.executor.instances", "1") 
        .config("spark.sql.debug.maxToStringFields", "100") 
        .config("spark.driver.maxResultSize", "32g") 
        .config("spark.executor.extraJavaOptions", "-XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m -XX:+CMSClassUnloadingEnabled -Xss4m")
        .config("spark.driver.extraJavaOptions", "-Xss4m") 
        .config("spark.sql.shuffle.partitions", "1200") 
        .config("spark.memory.fraction", "0.6") 
        .config("spark.memory.storageFraction", "0.4")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.locality.wait", "3s")
    .getOrCreate()
)




# Environment-specific configurations
if environment == "prod":
    spark_builder = spark_builder.config("spark.hadoop.fs.defaultFS", "hdfs://hd01.iadfalcon.com:8020").config("spark.network.timeout", "800s")
elif environment == "dev":
    spark_builder = spark_builder.config("spark.hadoop.fs.defaultFS", "hdfs://172.22.137.155:8020").config("spark.network.timeout", "1000s")
else:
    spark_builder = SparkSession.builder.appName("Parquet Reader")



print("\nReading this path .. : \n" + hdfs_path, "\n")

spark_df = spark.read.parquet(hdfs_path)
print(f"Time for reading the parquet: {get_time_elapsed():.4f} seconds \n\n")

print("count of rows:",spark_df.count())
print("schema  :\n",spark_df.printSchema())
filter_df=spark_df.select(
    "kafka",
    "kafka.payload",
    "kafka.version",
    "kafka.errors",
    # "kafka.errors.element",
    "kafka.payload_size"
    )
# .filter(col("kafka.payload").isNotNull())


filter_df.show(10)
filter_df=spark_df
# .select(
#     "kafka",
#     "kafka.payload",
#     "kafka.version",
#     "kafka.errors",
#     # "kafka.errors.element",
#     "kafka.payload_size"
#     )
# # .filter(col("kafka.payload").isNotNull())


filter_df.show(8)
column_count = len(filter_df.columns)
print(f"Number of columns: {column_count}")
