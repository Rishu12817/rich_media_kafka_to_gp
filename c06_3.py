import re
import time
import sys
from datetime import datetime, timedelta

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from sqlalchemy import create_engine
from pyspark.sql.functions import col, lit, udf, when, array, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, BooleanType

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
            # Create the Spark session
            spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()

            # Check if Spark is active
            if spark is None or spark.sparkContext is None:
                raise RuntimeError("Spark session could not be created!")
            
            offset1 = message.value
            f_batchid = convert_timestamp1(message.timestamp)
            f_batchid_with_sec = convert_timestamp2(message.timestamp)

            f_b_year = str(f_batchid)[0:4]
            f_b_year_month = str(f_batchid)[0:6]
            f_b_month = str(f_batchid)[4:6]
            f_b_day = str(f_batchid)[6:8]
            f_b_hour = str(f_batchid)[8:10]
            f_b_hourid = str(f_batchid)[0:10]
            f_b_min = '05'
            f_b_month = str(int(f_b_month))
            f_b_day = str(int(f_b_day))

            print("\n\n############# Start offset #################")
            
            hdfs_path = f"{hdfs_url}/temp/kafka02/topics_test/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/{f_batchid_with_sec}"

            decoded_message = offset1.decode('latin-1')

            ###### Functions ######
            def clean_message(msg):
                """ Remove non-ASCII characters and trim spaces. """
                return re.sub(r"[^\x20-\x7E]+", " ", msg).strip()

            def extract_RequestId(msg):
                """ Extract Request ID (32-64 hex chars). """
                match = re.search(r"\b[a-f0-9]{32,64}\b", msg, re.IGNORECASE)
                return match.group(0) if match else "Unknown"

            def extract_ip(msg):
                """ Extract all IP addresses. """
                matches = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", msg)
                return matches if matches else ["Unknown"]

            def extract_event_name(msg):
                """ Extract event name (at least 5 characters). """
                match = re.search(r"\b[A-Za-z0-9_-]{5,}\b", msg)
                return match.group(0) if match else "Unknown"

            def extract_HostName(msg):
                """ Extract host machine name. """
                match = re.search(r"\bDESKTOP-[A-Z0-9]+\b", msg)
                return match.group(0) if match else "Unknown"

            ###### Processing ######
            cleaned_message = clean_message(decoded_message)
            RequestId = extract_RequestId(cleaned_message) or "c55c10d167a548fb8788f04e2f996823133815053450228437"
            ips = extract_ip(cleaned_message) 
            event_name = extract_event_name(cleaned_message)
            HostName = extract_HostName(cleaned_message)

            HostIP = ips[0] if ips else "Unknown"
            ip_2 = ips[1] if len(ips) > 1 else "Unknown"
            Id = 7285308437620068352 if len(ips) > 1 else "Unknown"
            RequestTime = convert_timestamp3(message.timestamp)
            Time = convert_timestamp3(message.timestamp)

            default_values = {
                # "Id": f"{Id}",
                "Time": f"{Time}",
                "RequestTime": f"{RequestTime}",
                "AdId": "NULL",
                "AppSiteId": "NULL",
                "SubAppSiteId": "NULL",
                "DeviceBrandId": 0,
                "DeviceModelId": 0,
                "DeviceOsId": 0,
                "DeviceOSVersion": "NULL",
                "DeviceTypeId": 0,
                "CountryId": 0,
                "RegionId": 0,
                "CityId": 0,
                "MobileOperatorId": 0,
                "Type": 0,
                "EventCode": f"{event_name}",
                "UserId": "NULL",
                "RequestId": f"{RequestId}",
                "AdCreativeUnitId": "NULL",
                "KeywordIds": "NULL",
                "U_KeywordIds": "NULL",
                "AdTypeIds": "NULL",
                "GenderId": 0,
                "U_GenderId": 0,
                "Age": 0,
                "U_Age": 0,
                "EnvironmentType": 0,
                "DeviceOrientationId": 0,
                "AdResponseFormatId": 0,
                "LanguageId": 0,
                "RequestVersion": "NULL",
                "IP": HostIP,
                "ClientServerIP": ip_2,
                "CampaignType": 0,
                "HttpRefererId": "NULL",
                "IsWiFi": False,
                "IsOperaBrowser": False,
                "IsProxyTraffic": False,
                "Latitude": "NULL",
                "Longitude": "NULL",
                "FraudErrorCode": 0,
                "XForwardedFor": "NULL",
                "PartnerRefId": "NULL",
                "ChannelId": 0,
                "HostIP": HostIP,
                "HostName": HostName,
                "CreativeUnitIds": "NULL",
                "Demand_MappingId": "NULL",
                "Deals_AdCreativeUnits": {},
                "RTBSourceTID": "NULL",
                "PartnerImpTagId": "NULL",
                "BidFloor": 0.0,
                "MetaData": {
                    "AdCreativeUnitsData": {},
                    "AppSiteData": {"AccountId": 0},
                    "UndetectedDeviceData": [],
                    "RichMediaTransaction": {"EventInfo": "NULL", "DeviceId": "NULL"}
                },
                "AudienceSegmentDataBillSummary": "NULL",
                "PartnerSDK": "NULL",
                "PartnerSDKVersion": "NULL",
                "TrackUser": "NULL",
                "AdCreativeUnitsFormat": {},
                "IsInterstitial": False,
                "IsBillable": False,
                "PartnerUserId": "NULL",
                "PartnerBuyerUserId": "NULL",
                "AuctionType": 0,
                "ImpressionMetrics": [],
                "Trace": "NULL",
                "SubPublisherData": "NULL",
                "SSPPartnerId": "NULL",
                "BillingMetaData": "NULL",
                "IsCreativeUnitsDerived": False,
                "IsGoogleProxy": False,
                "AdMarkup": "NULL",
                "CellularType": 0,
                "BillableCost": 0.0,
                "AdjustedNetCost": 0.0,
                "NetCost": 0.0,
                "spend": 0.0,
                "AdFalconRevenue": 0.0,
                "GrossCost": 0.0,
                "CostItems": "NULL",
                "AgencyRevenue": 0.0,
                "CoordinatesSourceType": 0,
                "CoordinatesIsSystemCalculated": "NULL",
                "Conversion": "NULL",
                "ExtraUserData": {
                    "TagId": "NULL",
                    "Language": "NULL",
                    "ScreenWidth": "NULL",
                    "ScreenHeight": "NULL",
                    "Density": "NULL"
                },
                "OperatorDetectedByMNC": False,
                "ExternalDSPs": [],
                "IsExternalDSPRequested": False,
                "DeviceId": "NULL",
                "PartnerCampaignIds": "NULL",
                "UserAgentKey": 0,
                "IABCategories": "NULL",
                "Event_DeviceData": {
                    "DeviceBrandId": 0,
                    "DeviceOSId": 0,
                    "DeviceModelId": 0,
                    "DeviceOSVersion": "NULL",
                    "DeviceTypeId": 0,
                    "Custom_DeviceCapabilityIds": []
                },
                "Event_LocationData": {
                    "CountryId": 0,
                    "RegionId": 0,
                    "OperatorId": 0,
                    "CityId": 0
                },
            }


            # Debugging prints
            for key, value in {"f_batchid": f_batchid, "f_batchid_with_sec": f_batchid_with_sec, "RequestId": RequestId,
                            "IP Address 1": HostIP, "IP Address 2": ip_2, "Id": Id, "Time": Time,
                            "RequestTime": RequestTime}.items():
                print(f"{key}: {value}")

            for key, value in default_values.items():
                print(f"{key}: {value}")

            # Creating Data
            data = [(Id, *default_values.values())]

            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType, MapType

            try:
                schema = StructType([
                    StructField("Id", StringType(), True),
                    *(StructField(
                        key, 
                        BooleanType() if isinstance(value, bool) else 
                        StringType() if isinstance(value, str) else 
                        IntegerType() if isinstance(value, int) else 
                        FloatType() if isinstance(value, float) else 
                        ArrayType(StringType()) if isinstance(value, list) else 
                        MapType(StringType(), StringType()) if isinstance(value, dict) else 
                        StringType(),  # Default to StringType() for unknown types
                        True
                    ) for key, value in default_values.items())
                ])

                df = spark.createDataFrame(data, schema=schema)

            except Exception as e:
                print(f"\n--------------\nError: {e}")

            df.show(truncate=False)
            df.write.mode("append").parquet(hdfs_path)
            print("hdfs_path : ", hdfs_path)
            print("\n############# offset End #################\n\n")
            spark.stop()

        except Exception as e:
            print(f"An error occurred: {str(e)}")

        except UnicodeDecodeError:
            print("\n\nFailed to decode message as latin-1, it might be Protobuf or Avro\n\n")

except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
