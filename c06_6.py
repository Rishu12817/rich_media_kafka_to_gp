import json
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

dummy_data = [{'Version': 10, 'Id': 7287347004814921728, 'Time': datetime.now().isoformat(), 'RequestTime': datetime.now().isoformat(), 'AdId': 1234567890, 'AppSiteId': 1, 'SubAppSiteId': 2, 'DeviceBrandId': 3, 'DeviceModelId': 3, 'DeviceOsId': 3, 'DeviceOSVersion': '10.0', 'DeviceTypeId': 4, 'Custom_DeviceCapabilityIds': ['cap1', 'cap2'], 'CountryId': 1, 'RegionId': 2, 'CityId': 3, 'MobileOperatorId': 1, 'Type': 18, 'EventCode': 'right', 'UserId': str(uuid.uuid4()), 'RequestId': str(uuid.uuid4()), 'AdCreativeUnitId': 56789, 'KeywordIds': [1, 2, 3], 'U_KeywordIds': [4, 5], 'AdTypeIds': [6], 'GenderId': 1, 'U_GenderId': 1, 'Age': 30, 'U_Age': 30, 'CustomParameters': {'param1': 'value1', 'param2': 'value2'}, 'EnvironmentType': 0, 'DeviceOrientationId': 1, 'AdResponseFormatId': 1, 'BlackBerryVendorId': 1234, 'LanguageId': 1, 'RequestVersion': 1, 'IP': '192.168.1.1', 'ClientServerIP': '192.168.1.2', 'CampaignType': 0, 'HttpRefererId': 'referer123', 'IsWiFi': True, 'IsOperaBrowser': False, 'IsProxyTraffic': False, 'IsBlackBerry': False, 'Latitude': 40.7128, 'Longitude': -74.0060, 'FraudErrorCode': 16, 'XForwardedFor': None, 'PartnerRefId': 'partner123', 'ChannelId': 1, 'HostIP': '172.22.112.108', 'HostName': 'DESKTOP-MDKCIE7', 'StatColumnName': 'column1', 'CreativeUnitIds': [123, 456], 'Demand_MappingId': 789, 'Deals_AdCreativeUnits': {'deal1': 'value1', 'deal2': 'value2'}, 'RTBSourceTID': 'rtbsource123', 'PartnerImpTagId': 'partnerimp123', 'BidFloor': 0.5, 'metadata': {'adcreativeunitsdata': [{'key': 1, 'value': {'adid': 1, 'adgroupid': 2, 'campaignid': 3, 'accountid': 4, 'campaigntype': 1, 'advertiserid': 5, 'advassociationid': 6, 'retail': {'productid': 7}}}]}, 'appsiteinfo': {'accountid': 8}, 'undetecteddevicedata': [{'brandname': 'Apple', 'modelname': 'iPhone 14', 'source': 1}], 'AudienceSegmentDataBillSummary': {'summary1': 'value1'}, 'VideoData': {'MinDuration': 30, 'MaxDuration': 120, 'Width': 1920, 'Height': 1080}, 'NativeData': {'IconWidth': 48, 'IconHeight': 48}, 'PartnerSDK': 'SDKv1', 'PartnerSDKVersion': '1.0.0', 'TrackUser': 'usertrack1', 'AdCreativeUnitsFormat': 'format1', 'IsInterstitial': False, 'IsBillable': False, 'PartnerUserId': 'partneruser123', 'PartnerBuyerUserId': 'partnerbuyer123', 'AuctionType': 0, 'ImpressionMetrics': {'metric1': 'value1'}, 'Trace': 'trace1', 'SubPublisherData': 'subpublisher123', 'SSPPartnerId': 'ssp123', 'BillingMetaData': {'billdata': 'value1'}, 'IsCreativeUnitsDerived': False, 'IsGoogleProxy': False, 'AdMarkup': '<admarkup></admarkup>', 'CellularType': 0, 'BillableCost': 0.0, 'AdjustedNetCost': 0.0, 'NetCost': 0.0, 'AdFalconRevenue': 0.0, 'GrossCost': 0.0, 'CostItems': {'item1': 'cost1'}, 'AgencyRevenue': 0.0, 'CoordinatesSourceType': 0, 'CoordinatesIsSystemCalculated': None, 'Conversion': {'conversion_type': 'type1'}, 'ExtraUserData': {'TagId': 123, 'Language': 'en', 'ScreenWidth': 1920, 'ScreenHeight': 1080, 'Density': 2.0}, 'UserAliasId': 'alias123', 'OperatorDetectedByMNC': False, 'ExternalDSPs': None, 'IsExternalDSPRequested': False, 'DeviceId': 'device123', 'PartnerCampaignIds': ['campaign1', 'campaign2'], 'UserAgentKey': 3264627309505647518, 'IABCategories': {'category1': 'value1'}, 'Event_DeviceData': {'DeviceBrandId': 101, 'DeviceOSId': 303}, 'Event_LocationData': {'CountryId': 1, 'RegionId': 2, 'OperatorId': 1, 'CityId': 3}, 'CreativeProtocolIds': [1, 2], 'PageUrl': 'https://example.com', 'ProvidersSegments': ['segment1'], 'SponsoredAdData': {'ad1': 'sponsored1'}, 'AdCreativeIds_SearchKeywordIds': ['keyword1'], 'BiddingData': {'bid1': 0.1}}]
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
    # StructField("AdCreativeUnitsFormat", StringType(), True),
    StructField("adcreativeunitsformat", ArrayType(
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
# Initialize Spark session once outside the loop
spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()

# Check if Spark is active
if spark is None or spark.sparkContext is None:
    raise RuntimeError("Spark session could not be created!")


# List to store messages before writing
batch_data = []

try:
    for message in consumer:
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
            hdfs_path = f"{hdfs_url}/temp/kafka02/topics_test01/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/{f_batchid_with_sec}"

            import json
            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType

            # Assuming `batch_data`, `spark`, `schema`, `hdfs_path`, and `message` are defined elsewhere

            print("\n\n############# Start offset #################")
            print(" message : fetched\n")

            # Ensure message.value is not None or empty before decoding
            if message.value and message.key is None:
                offset1 = message.value
                print(" offset1 : : fetched\n")

                # Decode the message
                decoded_message = offset1.decode('utf-8')
                print(" decoded_message : done \n")

                # Load the JSON message into a Python dictionary
                default_values = json.loads(decoded_message)

                # Convert fields to correct types (ensure these keys exist)
                default_values["Version"] = int(default_values.get("Version", 0))
                default_values["Id"] = int(default_values.get("Id", 0))

                # Append to batch_data
                batch_data.append(default_values)

                print("count batch length : ", len(batch_data))

                # # Check if batch data is empty and handle accordingly (optional)
                # if len(batch_data) == 0:
                #     print("⚠️ Warning: batch_data is empty, forcing write with dummy row")
                #     batch_data.append(dummy_data)  # Ensure dummy_data is defined elsewhere

                # Write data in batches when batch size is met
                if len(batch_data) >= 10:  # Adjust batch size as needed
                    print("creating batch df with sparse matrix")
                    # Convert the batch_data to a DataFrame using the predefined schema
                    df = spark.createDataFrame(batch_data, schema=schema)
                    print("Created df successfully with sparse matrix")
                    # df.show()

                    # Write the DataFrame to HDFS as Parquet (ensure hdfs_path is defined)
                    df.write.mode("append").parquet(hdfs_path)
                    print("Data written at this location :\n",hdfs_path)

                    # Clear the batch data after writing
                    batch_data.clear()

            else:
                print("No message found in Kafka message.")


        except Exception as e:
            print(f"Error processing message: {e}")



except KeyboardInterrupt:
    print("Stopping consumer...")

# Write any remaining data before exiting
if batch_data:
    df = spark.createDataFrame(batch_data, schema=schema)
    df.write.mode("append").parquet(hdfs_path)
