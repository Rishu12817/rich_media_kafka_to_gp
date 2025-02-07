import json
import random
import re
import time
import sys
from datetime import datetime, timedelta
import uuid

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from sqlalchemy import create_engine
from pyspark.sql.functions import col, lit, udf, when, array, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, BooleanType
from pyspark.sql.types import LongType, DoubleType, ArrayType
import re
import psycopg2
import psycopg2.extras
import pandas as pd
import logging
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

environment_var = get_arg_value("-e") or config.env
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
    spark_builder = spark_builder.config("spark.hadoop.fs.defaultFS", hdfs_url ).config("spark.network.timeout", "80000s")
elif environment == "dev" or environment == "ldev":
    hdfs_url = "hdfs://172.22.137.155:8020"
    spark_builder = spark_builder.config("spark.hadoop.fs.defaultFS", hdfs_url).config("spark.network.timeout", "100000s")
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


# Define schema to handle StructType
schema = StructType([
    StructField("Version", LongType(), True),
    StructField("Id", LongType(), True),
    StructField("Time", StringType(), True),
    StructField("RequestTime", StringType(), True),
    StructField("AdId", StringType(), True),
    StructField("AppSiteId", StringType(), True),
    StructField("SubAppSiteId", StringType(), True),
    StructField("DeviceBrandId", StringType(), True),
    StructField("DeviceModelId", StringType(), True),
    StructField("DeviceOsId", StringType(), True),
    StructField("DeviceOSVersion", StringType(), True),
    StructField("DeviceTypeId", StringType(), True),
    StructField("Custom_DeviceCapabilityIds", ArrayType(StringType()), True),
    StructField("CountryId", StringType(), True),
    StructField("RegionId", StringType(), True),
    StructField("CityId", StringType(), True),
    StructField("MobileOperatorId", StringType(), True),
    StructField("Type", LongType(), True),
    StructField("EventCode", StringType(), True),
    StructField("UserId", StringType(), True),
    StructField("RequestId", StringType(), True),
    StructField("AdCreativeUnitId", StringType(), True),
    StructField("KeywordIds", ArrayType(StringType()), True),
    StructField("U_KeywordIds", ArrayType(StringType()), True),
    StructField("AdTypeIds", ArrayType(StringType()), True),
    StructField("GenderId", StringType(), True),
    StructField("U_GenderId", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("U_Age", StringType(), True),
    StructField("CustomParameters", StringType(), True),
    StructField("EnvironmentType", LongType(), True),
    StructField("DeviceOrientationId", StringType(), True),
    StructField("AdResponseFormatId", StringType(), True),
    StructField("BlackBerryVendorId", StringType(), True),
    StructField("LanguageId", StringType(), True),
    StructField("RequestVersion", StringType(), True),
    StructField("IP", StringType(), True),
    StructField("ClientServerIP", StringType(), True),
    StructField("CampaignType", LongType(), True),
    StructField("HttpRefererId", StringType(), True),
    StructField("IsWiFi", StringType(), True),
    StructField("IsOperaBrowser", StringType(), True),
    StructField("IsProxyTraffic", StringType(), True),
    StructField("IsBlackBerry", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longitude", StringType(), True),
    StructField("FraudErrorCode", LongType(), True),
    StructField("XForwardedFor", StringType(), True),
    StructField("PartnerRefId", StringType(), True),
    StructField("ChannelId", StringType(), True),
    StructField("HostIP", StringType(), True),
    StructField("HostName", StringType(), True),
    StructField("StatColumnName", StringType(), True),
    StructField("CreativeUnitIds", ArrayType(StringType()), True),
    StructField("Demand_MappingId", StringType(), True),
    StructField("Deals_AdCreativeUnits", StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("RTBSourceTID", StringType(), True),
    StructField("PartnerImpTagId", StringType(), True),
    StructField("BidFloor", DoubleType(), True),
    StructField("metadata", StructType([
        StructField("adcreativeunitsdata", ArrayType(StructType([
            StructField("key", IntegerType(), True),
            StructField("value", StructType([
                StructField("adid", IntegerType(), True),
                StructField("adgroupid", IntegerType(), True),
                StructField("campaignid", IntegerType(), True),
                StructField("accountid", IntegerType(), True),
                StructField("campaigntype", IntegerType(), True),
                StructField("advertiserid", IntegerType(), True),
                StructField("advassociationid", IntegerType(), True),
                StructField("retail", StructType([
                    StructField("productid", IntegerType(), True)
                ]), True),
            ]), True),
        ]), True)),
        StructField("appsiteinfo", StructType([
            StructField("accountid", IntegerType(), True)
        ]), True),
        StructField("undetecteddevicedata", ArrayType(StructType([
            StructField("brandname", StringType(), True),
            StructField("modelname", StringType(), True),
            StructField("source", IntegerType(), True)
        ]), True))
    ]), True),

    StructField("AudienceSegmentDataBillSummary", StringType(), True),
    StructField("VideoData", StructType([
        StructField("MinDuration", StringType(), True),
        StructField("MaxDuration", StringType(), True),
        StructField("Width", StringType(), True),
        StructField("Height", StringType(), True),
        StructField("MinBitRate", StringType(), True),
        StructField("MaxBitRate", StringType(), True),
        StructField("DeliveryMethodIds", StringType(), True),
        StructField("MIMETypeIds", StringType(), True),
        StructField("IsRewarded", StringType(), True),
        StructField("PlacementTypeId", StringType(), True),
        StructField("InStreamPositionId", StringType(), True),
        StructField("SkippableAdOptionIds", StringType(), True),
        StructField("PlaybackMethodId", StringType(), True),
        StructField("SupportEndCard", StringType(), True),
        StructField("CompanionTypes", StringType(), True)
    ]), True),
    StructField("NativeData", StructType([
        StructField("IconWidth", StringType(), True),
        StructField("IconHeight", StringType(), True),
        StructField("IsLargerIconAllowed", StringType(), True),
        StructField("IsIconMandatory", StringType(), True),
        StructField("IconMIMETypeIds", StringType(), True),
        StructField("ImageWidth", StringType(), True),
        StructField("ImageHeight", StringType(), True),
        StructField("IsLargerImageAllowed", StringType(), True),
        StructField("IsImageMandatory", StringType(), True),
        StructField("ImageMIMETypeIds", StringType(), True),
        StructField("TitleMaxLength", StringType(), True),
        StructField("IsTitleMandatory", StringType(), True),
        StructField("DescriptionMaxLength", StringType(), True),
        StructField("IsDescriptionMandatory", StringType(), True),
        StructField("ActionTextMaxLength", StringType(), True),
        StructField("IsActionTextMandatory", StringType(), True),
        StructField("IsRatingMandatory", StringType(), True),
        StructField("SponsoredMaxLength", StringType(), True),
        StructField("IsSponsoredMandatory", StringType(), True)
    ]), True),
    StructField("PartnerSDK", StringType(), True),
    StructField("PartnerSDKVersion", StringType(), True),
    StructField("TrackUser", StringType(), True),
    StructField("Adcreativeunitsformat", ArrayType(
        StructType([
            StructField("key", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])
    ), True),
    StructField("IsInterstitial", StringType(), True),
    StructField("IsBillable", StringType(), True),
    StructField("PartnerUserId", StringType(), True),
    StructField("PartnerBuyerUserId", StringType(), True),
    StructField("AuctionType", LongType(), True),
    StructField("ImpressionMetrics", ArrayType(StringType()), True),
    StructField("Trace", StringType(), True),
    StructField("SubPublisherData", StringType(), True),
    StructField("SSPPartnerId", StringType(), True),
    StructField("BillingMetaData", StringType(), True),
    StructField("IsCreativeUnitsDerived", StringType(), True),
    StructField("IsGoogleProxy", StringType(), True),
    StructField("AdMarkup", StringType(), True),
    StructField("CellularType", LongType(), True),
    StructField("BillableCost", DoubleType(), True),
    StructField("AdjustedNetCost", DoubleType(), True),
    StructField("NetCost", DoubleType(), True),
    StructField("AdFalconRevenue", DoubleType(), True),
    StructField("GrossCost", DoubleType(), True),
    StructField("CostItems", StringType(), True),
    StructField("AgencyRevenue", DoubleType(), True),
    StructField("CoordinatesSourceType", LongType(), True),
    StructField("CoordinatesIsSystemCalculated", StringType(), True),
    StructField("Conversion", StringType(), True),
    StructField("ExtraUserData", StructType([
        StructField("TagId", StringType(), True),
        StructField("Language", StringType(), True),
        StructField("ScreenWidth", StringType(), True),
        StructField("ScreenHeight", StringType(), True),
        StructField("Density", StringType(), True)
    ]), True),
    StructField("UserAliasId", StringType(), True),
    StructField("OperatorDetectedByMNC", StringType(), True),
    StructField("ExternalDSPs", ArrayType(StringType()), True),
    StructField("IsExternalDSPRequested", StringType(), True),
    StructField("DeviceId", StringType(), True),
    StructField("PartnerCampaignIds", StringType(), True),
    StructField("UserAgentKey", LongType(), True),
    StructField("IABCategories", StringType(), True),
    StructField("Event_DeviceData", StructType([
        StructField("DeviceBrandId", StringType(), True),
        StructField("DeviceOSId", StringType(), True),
        StructField("DeviceModelId", StringType(), True),
        StructField("DeviceOSVersion", StringType(), True),
        StructField("DeviceTypeId", StringType(), True),
        StructField("Custom_DeviceCapabilityIds", ArrayType(StringType()), True)
    ]), True),
    StructField("CreativeProtocolIds", StringType(), True),
    StructField("PageUrl", StringType(), True),
    StructField("ProvidersSegments", StringType(), True),
    StructField("SponsoredAdData", StringType(), True),
    StructField("AdCreativeIds_SearchKeywordIds", StringType(), True),
    StructField("BiddingData", StringType(), True),
    StructField("UserAgent", StringType(), True),
    StructField("HttpReferer", StringType(), True)])


# List to store messages before writing
batch_data = []
# Get current time
current_time = datetime.now()

# Calculate start and end of the 2nd previous hour
second_previous_hour = current_time - timedelta(hours=1)
# current_time
start_of_second_previous_hour = second_previous_hour.replace(minute=0, second=0, microsecond=0)
end_of_second_previous_hour = start_of_second_previous_hour + timedelta(hours=1)

print(f"Filtering messages between {start_of_second_previous_hour} and {end_of_second_previous_hour}")


try:
    for message in consumer:

        message_timestamp = datetime.fromtimestamp(message.timestamp / 1000)  # Convert Kafka timestamp from milliseconds to seconds
        
        # Process only if the message timestamp falls within the 1st previous hour
        # if True:
        if start_of_second_previous_hour <= message_timestamp < end_of_second_previous_hour:
            # print(f"Processing message: {message.value} at {message_timestamp}")

            try:
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
                hdfs_path = f"{hdfs_url}/dw/kafka/topics/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/{f_batchid_with_sec}"

                # Initialize Spark session once outside the loop
                spark = SparkSession.builder.appName("KafkaConnectService").getOrCreate()

                # Check if Spark is active
                if spark is None or spark.sparkContext is None:
                    raise RuntimeError("Spark session could not be created!")


                import json
                from pyspark.sql import SparkSession
                from pyspark.sql.types import StructType

                # Assuming `batch_data`, `spark`, `schema`, `hdfs_path`, and `message` are defined elsewhere

                if flag: print("\n\n############# Start offset #################")
                # Ensure message.value is not None or empty before decoding
                if message.value and message.key is None:
                    offset1 = message.value
                    if flag: print(" offset1 : : fetched\n")

                    # Decode the message
                    decoded_message = offset1.decode('utf-8')
                    if flag: print(" decoded_message : done \n")

                    # Load the JSON message into a Python dictionary
                    default_values = json.loads(decoded_message)

                    # Convert fields to correct types (ensure these keys exist)
                    default_values["Version"] = int(default_values.get("Version", 0))
                    default_values["Id"] = int(default_values.get("Id", 0))

                    batch_data.append(default_values)

                    if flag: print("count batch length : ", len(batch_data))
                    # # Check if batch data is empty and handle accordingly (optional)
                    # if len(batch_data) == 0:
                    #     print("⚠️ Warning: batch_data is empty, forcing write with dummy row")
                    #     batch_data.append(dummy_data)  # Ensure dummy_data is defined elsewhere

                    # Write data in batches when batch size is met
                    # if len(batch_data) >= 15:  # Adjust batch size as needed
                    if flag: print("creating batch df with sparse matrix")
                    # Convert the batch_data to a DataFrame using the predefined schema
                    df = spark.createDataFrame(batch_data, schema=schema) 
                    if flag: print("Created df successfully with sparse matrix")
                    # df.show()

                    # Write the DataFrame to HDFS as Parquet (ensure hdfs_path is defined)
                    df.write.mode("append").parquet(hdfs_path)
                    if flag: print("Data written at this location :\n",hdfs_path)

                    # Clear the batch data after writing
                    batch_data.clear()

                    if flag: print("\n\n############# Start offset #################")
                else:
                    print("No message found in Kafka message.")


            except Exception as e:
                print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("Stopping consumer...")

spark.stop()
consumer.close()

