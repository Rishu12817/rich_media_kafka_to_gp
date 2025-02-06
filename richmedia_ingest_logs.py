import re
import time
import sys
from datetime import datetime, timedelta

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf, when, array, split
from sqlalchemy import create_engine
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType
from pyspark.sql.utils import AnalysisException

import psycopg2
import psycopg2.extras
import pandas as pd

sys.path.append("..")
import config

# Set environment flags
flag = True
limit = True
aeroflag = False
global count
count = 0

# Store the start time when the script starts
start_time = time.time()

def get_time_elapsed():
    current_time = time.time()
    elapsed_time = current_time - start_time
    return elapsed_time

def log_time(previous_time, label=""):
    current_time = time.time()
    elapsed_time = current_time - previous_time
    print(f"{label} - Time taken: {elapsed_time:.2f} seconds")
    return current_time

def get_arg_value(arg):
    try:
        idx = sys.argv.index(arg)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return None

def generate_custom_datetime_format():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d%H") + "05"

environment_var = get_arg_value("-e")
valid_envs = {"dev", "prod", "stg", "ldev"}
if environment_var not in valid_envs:
    print("Pass parameter -e as dev, prod, stg, or ldev")
    sys.exit(1)

environment = environment_var
now = datetime.now() - timedelta(days=1)
year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')

# Initialize Spark session with environment-specific configurations
spark_builder = (
    SparkSession.builder
        .appName("Adfalcon")
        .config("spark.executor.memory", "64g")
        .config("spark.driver.memory", "64g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.driver.maxResultSize", "32g")
        .config("spark.executor.extraJavaOptions", "-XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m -XX:+CMSClassUnloadingEnabled -Xss4m")
        .config("spark.sql.shuffle.partitions", "1200")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.4")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.locality.wait", "3s")
)

if environment == "prod":
    hdfs_url = "hdfs://hd01.iadfalcon.com:8020"
    spark_builder = spark_builder.config("spark.hadoop.fs.defaultFS", hdfs_url ).config("spark.network.timeout", "800s")
elif environment == "dev" or environment == "ldev":
    hdfs_url = "hdfs://172.22.137.155:8020"
    spark_builder = spark_builder.config("spark.hadoop.fs.defaultFS", hdfs_url).config("spark.network.timeout", "1000s")
else:
    spark_builder = spark_builder.appName("Parquet Reader")




# Kafka configuration
topic_name = 'richmedia_ingest_logs'
group_id = 'richmedia_ingest_logs_consumer_group'


# Create Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=config.kafka["bootstrap.servers"],
    group_id=group_id,
    enable_auto_commit=False,
    auto_offset_reset='earliest'
)

consumer.poll(timeout_ms=1000)  # Poll to ensure partitions are assigned
for partition in consumer.assignment():
    consumer.seek_to_beginning(partition)

print(f"Consuming messages from Kafka topic '{topic_name}'...")

from datetime import datetime, timedelta
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_last_hour_start_and_end():
    now = datetime.now()
    last_hour = now - timedelta(hours=1)
    start_of_last_hour = last_hour.replace(minute=0, second=0, microsecond=0)
    end_of_last_hour = start_of_last_hour + timedelta(hours=1)
    return start_of_last_hour.timestamp(), end_of_last_hour.timestamp()

last_hour_start, last_hour_end = get_last_hour_start_and_end()

def convert_timestamp1(timestamp_ms):
    """
    :param timestamp_ms: Unix timestamp in milliseconds (int or float)
    :return: Formatted UTC date string (YYYY-MM-DD HH:MM:SS UTC)
    """
    timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
    readable_date = datetime.utcfromtimestamp(timestamp_sec)
    return readable_date.strftime('%Y%m%d%H05')

def convert_timestamp2(timestamp_ms):
    """
    :param timestamp_ms: Unix timestamp in milliseconds (int or float)
    :return: Formatted UTC date string (YYYY-MM-DD HH:MM:SS UTC)
    """
    timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
    readable_date = datetime.utcfromtimestamp(timestamp_sec)
    return readable_date.strftime('%Y%m%d%H%M%S')

def convert_timestamp3(timestamp_ms):
    """
    :param timestamp_ms: Unix timestamp in milliseconds (int or float)
    :return: Formatted UTC date string (YYYY-MM-DDTHH:MM:SS.ssssssZ)
    """
    timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
    readable_date = datetime.utcfromtimestamp(timestamp_sec)
    return readable_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

try:
    for message in consumer:
        try:
            offset1 = message.value

            try:
                if message.key is None:  # Only print if msg.key is None
                    print("\n\n############# Start offset #################")
                    decoded_message = offset1.decode('latin-1')
                    print(message, "\n@@@@@@@@@@@@@@@@@@@@\n", message)
                    print(decoded_message, "\n", convert_timestamp3(message.timestamp))
                    print("\n\n############# End offset #################")
            except Exception as e:
                print(f"An error occurred: {str(e)}")

        except Exception as e:
            print(f"An error occurred: {str(e)}")

        except UnicodeDecodeError:
            print("\n\nFailed to decode message as latin-1, it might be Protobuf or Avro\n\n")

except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
