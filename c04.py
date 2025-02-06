import re
from kafka import KafkaConsumer
import binascii
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf, when
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




# Kafka configuration
topic_name = 'richmedia_ingest_logs'
group_id = 'richmedia_ingest_logs_consumer_group'

# Create Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=config.kafka["bootstrap.servers"],
    group_id=group_id,
    enable_auto_commit=False,  # Disable auto-commit to manually control offsets
    auto_offset_reset='earliest'  # Start reading from the beginning
)

# Manually seek to the earliest offset for all assigned partitions
consumer.poll(timeout_ms=1000)  # Poll to ensure partitions are assigned
for partition in consumer.assignment():
    consumer.seek_to_beginning(partition)


print(f"Consuming messages from Kafka topic '{topic_name}'...")

import datetime

def convert_timestamp(timestamp_ms):
    """
    :param timestamp_ms: Unix timestamp in milliseconds (int or float)
    :return: Formatted UTC date string (YYYY-MM-DD HH:MM:SS UTC)
    """
    timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
    readable_date = datetime.datetime.utcfromtimestamp(timestamp_sec)
    return readable_date.strftime('%Y%m%d%H05')
    # return readable_date.strftime('%Y-%m-%d %H:%M:%S UTC')

try:
    for message in consumer:
        # Decode message assuming it's UTF-8 encoded JSON
        try:
            offset1 = message.value
            f_batchid = convert_timestamp(message.timestamp)
            f_b_year = str(f_batchid)[0:4]
            f_b_year_month = str(f_batchid)[0:6]
            f_b_month = str(f_batchid)[4:6]
            f_b_day = str(f_batchid)[6:8]
            f_b_hour = str(f_batchid)[8:10]
            f_b_hourid = str(f_batchid)[0:10]
            f_b_min = '05'
            # Convert month and day to integers to remove leading zeros
            f_b_month = str(int(f_b_month))
            f_b_day = str(int(f_b_day))

            # Set HDFS path based on environment
            # hdfs_path = f"hdfs://172.22.137.155:8020/dw/kafka/topics/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/"

            print("\n\n############# Start offset #################")
            
            hdfs_path = f"hdfs://172.22.137.155:8020/temp/kafkat/topics/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/"

            decoded_message = offset1.decode('latin-1')
            print(f"\nReceived timestamp: \n{convert_timestamp(message.timestamp)}\n","\nDecoded Message:\n", decoded_message)

                        
            # Function to extract batch status
            def extract_batch_status(msg):
                match = re.search(r"=␦\s*([\w\-\.]+)", msg)
                return match.group(1) if match else ""

            # Function to extract UUID
            def extract_uuid(msg):
                match = re.search(r"¢([a-f0-9]{32})", msg)
                return match.group(1) if match else ""

            # Function to extract IP addresses
            def extract_ip(msg):
                matches = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", msg)
                return matches if matches else ["", ""]  # Ensure two IPs exist

            # Function to extract device ID
            def extract_device_id(msg):
                match = re.search(r"([A-Za-z0-9\-]+)", msg)
                return match.group(1) if match else ""

            # Extract data safely
            batch_status = extract_batch_status(decoded_message) or "Unknown"
            uuid = extract_uuid(decoded_message) or "Unknown"
            ip_1, ip_2 = extract_ip(decoded_message)
            device_id = extract_device_id(decoded_message) or "Unknown"

            # Print extracted values for debugging
            print(f"Batch Status: {batch_status}")
            print(f"UUID: {uuid}")
            print(f"IP Address 1: {ip_1}")
            print(f"IP Address 2: {ip_2}")
            print(f"Device ID: {device_id}")

            # Define schema for Spark DataFrame
            schema = StructType([
                StructField("batch_status", StringType(), True),
                StructField("uuid", StringType(), True),
                StructField("ip_1", StringType(), True),
                StructField("ip_2", StringType(), True),
                StructField("device_id", StringType(), True)
            ])

            # Create DataFrame
            data = [(batch_status, uuid, ip_1, ip_2, device_id)]

            # Ensure Spark session exists before creating a DataFrame
            if spark:
                df = spark.createDataFrame(data, schema=schema)
                df.show(truncate=False)
            else:
                print("Error: Spark session is not available.")


            print("hdfs_path : ",hdfs_path)
            print("\n############# offset End #################\n\n")
        except UnicodeDecodeError:
            print("\n\nFailed to decode message as latin-1, it might be Protobuf or Avro\n\n")

        spark.stop()    
        

except Exception as e:
    print(f"Error: {e}")


    


finally:
    consumer.close()





