import json
import re
import time
import sys
from datetime import datetime, timedelta
import uuid
import mysql.connector
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from sqlalchemy import create_engine
from pyspark.sql.functions import col, lit, udf, when, array, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import col, explode
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

f_batchid = generate_custom_datetime_format()
# f_batchid = 202502031205
f_b_year = str(f_batchid)[0:4]
f_b_year_month = str(f_batchid)[0:6]
f_b_month = str(f_batchid)[4:6]
f_b_day = str(f_batchid)[6:8]
f_b_hour = str(f_batchid)[8:10]
f_b_hourid = str(f_batchid)[0:10]
f_b_min = '05'
f_b_month = str(int(f_b_month))
f_b_day = str(int(f_b_day))

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
        .config("spark.executor.cores", "4")
        .config("spark.executor.instances", "4")
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


# Create the Spark session
spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()

# Check if Spark is active
if spark is None or spark.sparkContext is None:
    raise RuntimeError("Spark session could not be created!")


# f_batchid = convert_timestamp1(message.timestamp)
# f_batchid_with_sec = convert_timestamp2(message.timestamp)
# f_b_year = str(f_batchid)[0:4]
# f_b_year_month = str(f_batchid)[0:6]
# f_b_month = str(f_batchid)[4:6]
# f_b_day = str(f_batchid)[6:8]
# f_b_hour = str(f_batchid)[8:10]
# f_b_hourid = str(f_batchid)[0:10]
# f_b_min = '05'
# f_b_month = str(int(f_b_month))
# f_b_day = str(int(f_b_day))
hdfs_path = f"{hdfs_url}/dw/kafka/topics/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/*"

hdfs_path = f"{hdfs_url}/temp/kafka02/topics_test05/richmedia_ingest_logs/batches/202502/batchid=202502061305/hourid=2025020613/minute=00/*"


print("Starting pipeline...")
print("Start time: ",start_time)
spark_df = spark.read.parquet(hdfs_path)

if flag: spark_df.show(1)
if flag: spark_df.printSchema()

# Select unique entries in the column 'EventCode'
unique_event_codes = spark_df.select("EventCode").distinct().rdd.flatMap(lambda x: x).collect()

# Print each unique entry in a for loop
for event_code in unique_event_codes:
    if flag: print("Current EventCode is ",event_code)
    spark_df = spark.read.parquet(hdfs_path).where(f"eventcode = '{event_code}'")

    from pyspark.sql.functions import col, sum, count, countDistinct, explode
    # Exploding the adcreativeunitsdata array inside metadata
    spark_df = spark_df.withColumn("adcreativeunitsdata", explode(col("metadata.adcreativeunitsdata")))
    # Ensure nested columns are selected explicitly before grouping
    spark_df = spark_df.withColumn("adid", col("adcreativeunitsdata.value.adid")) \
                    .withColumn("adcreativeunits", col("adcreativeunitsdata.key")) \
                    .withColumn("adgroupid", col("adcreativeunitsdata.value.adgroupid")) \
                    .withColumn("campaignid", col("adcreativeunitsdata.value.campaignid")) \
                    .withColumn("accountid", col("adcreativeunitsdata.value.accountid")) \
                    .withColumn("campaigntype", col("adcreativeunitsdata.value.campaigntype")) \
                    .withColumn("advertiserid", col("adcreativeunitsdata.value.advertiserid")) \
                    .withColumn("advassociationid", col("adcreativeunitsdata.value.advassociationid")) 
    if flag: print(spark_df.columns)
    # Cast numeric columns to double
    numeric_cols = [
        "AdFalconRevenue",
        "BillableCost",
        "AdjustedNetCost",
        "NetCost",
        "GrossCost",
        "AgencyRevenue"
    ]

    for col_name in numeric_cols:
        spark_df = spark_df.withColumn(col_name, col(col_name).cast("double"))

    # Perform group by and aggregation
    agg_df = spark_df.groupBy(
        "time",
        "adcreativeunitsformat",
        "appsiteid",
        "subappsiteid",
        "devicebrandid",
        "devicemodelid",
        "deviceosid",
        "countryid",
        "regionid",
        "mobileoperatorid",
        "keywordids",
        "u_keywordids",
        "genderid",
        "age",
        "u_genderid",
        "environmenttype",
        "languageid",
        "cityid",
        "channelid",
        "hostip",
        "hostname",
        "creativeunitids",
        "costitems",
        "adid",
        "adgroupid",
        "campaignid",
        "accountid",
        "campaigntype",
        "advertiserid",
        "advassociationid",
        "EventCode",
        "adcreativeunits",
        "metadata"
    ).agg(
        sum(col("AdFalconRevenue")).alias("sum_adfalconrevenue"),
        sum(col("BillableCost")).alias("sum_billablecost"),
        sum(col("AdjustedNetCost")).alias("sum_adjustednetcost"),
        sum(col("NetCost")).alias("sum_netcost"),
        sum(col("GrossCost")).alias("sum_grosscost"),
        sum(col("AgencyRevenue")).alias("sum_agencyrevenue"),
        count("RequestId").alias("row_count"),
        countDistinct("DeviceId").alias("deviceid_count")
    )
    print("Spark data loaded to the dataframe successfully")
    if (flag) : agg_df.show(2)

    joined_df = agg_df

    # Show the joined DataFrame
    if (flag) : joined_df.show(10)
    agg_count= joined_df.count()
    if (flag) : print("agg_count: ",agg_count)
    if (agg_count) < 1 : 
        print("No data available.. exiting")
        spark.stop()
        sys.exit(1)

    # Establish connection to Greenplum
    gp_connection = psycopg2.connect(**config.gp_config)
    gp_cursor = gp_connection.cursor()

    # Get the current date in the format yyyymmdd
    f_batchid_str = str(f_batchid)
    # Extract date portion (yyyymmdd)
    dateid = f_batchid_str[:8]


    # considering sunday 1st day of the week
    dateid_datetime = datetime.strptime(dateid, '%Y%m%d')
    # Get the weekday (0 = Monday, 6 = Sunday) for the given date
    day_of_week = dateid_datetime.weekday()
    # Calculate the number of days to subtract to get to the previous Sunday
    days_to_subtract = (day_of_week + 1) % 7
    # Subtract the days to get to the previous Sunday
    last_sunday = dateid_datetime - timedelta(days=days_to_subtract)
    # Format the last Sunday as yyyymmdd
    weekid_str = last_sunday.strftime('%Y%m%d')
    # Convert weekid to a string
    weekid = str(weekid_str)


    hour_id = f_batchid_str[8:10]
    # Extract year and month and format as yyyymm01
    year_month = f_batchid_str[:6]
    monthid = f"{year_month}01"

    ## add dateid join datafram
    joined_df = joined_df.withColumn("dateid", lit(dateid))
    # Get the column names from the database table

    gp_cursor.execute("SELECT column_name FROM information_schema.columns")
                    #    WHERE table_name = 'fact_stat_d'")
    column_names = [row[0] for row in gp_cursor.fetchall()]


    # Set to keep track of generated keys
    generated_keys = set()


    # batch size for generated
    batch_size = 5000

    final_gp_insert_query_values_fsh = []
    tablename= "fact_stat_customevent_h" 

    final_gp_insert_query_fields_fsh = f"""INSERT INTO {tablename} (hour_key, day_key, dateid, hourid,     requesthourid, advaccountid,  appsiteid, subappsiteid, countryid, regionid, cityid, mobileoperatorid,   devicebrandid, devicemodelid, deviceosid, genderid, u_genderid, spend, appsiterevenue, adfalconrevenue,   languageid, environmenttype, netcost, adjustednetcost, billablecost, grosscost, agencyrevenue, Adid,  platformfee, datafee, thirdpartyfee, requests, requests_d, unfilledrequests, unfilledrequests_d, connectiontype, creativeunitgroupid, pubaccountid, channelid, CampaignId, campaigntype, adtypegroupid,  custom_event_count, dealid, wins, advertiserid, requestdateid, adgroupid, adcreativeunitid, keywordgroupid,   u_keywordgroupid, requests_dcr, requests_cr, pageviews, vcreativeviews, vstart, vfirstquartile, vmidpoint, vthirdquartile, vcomplete, conversions, requests_ad, requests_ag, requests_ca, requests_dad, requests_dag, requests_dca, totaldataproviderrevenue, advassociationid, conv_pr, conv_pr_ct, conv_pr_vt, conv_ot, conv_ot_ct, conv_ot_vt, conv_pr_rev, conv_pr_ct_rev, conv_pr_vt_rev, conv_ot_rev, agegroupid, u_agegroupid, event_name, unique_event_count,totaldataprice,totaladfalconrevenue,impressions,clicks) VALUES
    """ 

    joined_df_list = joined_df.collect()
    # Iterate over the DataFrame rows
    for j, row in  enumerate(joined_df_list):
        # Generate unique UUIDs for day_key, week_key, and month_key
        hour_key = str(uuid.uuid4())
        day_key = str(uuid.uuid4())
        hourid = hour_id

        # Ensure uniqueness of keys
        while hour_key in generated_keys:
            hour_key = str(uuid.uuid4())

        # Add keys to the set of generated keys
        generated_keys.add(hour_key)
        def find_name_id_by_age(age):
            data = [
            (1, 3883, 18, 24),
            (2, 3884, 25, 34),
            (3, 3885, 35, 44),
            (4, 3886, 45, 54),
            (5, 3887, 55, 64),
            (6, 3888, 65, 127)
            ]

            for entry in data:
                min_age = entry[2]
            max_age = entry[3]

            if min_age is not None and max_age is not None:
            # if min_age <= age <= max_age:
                return entry[0]
            return None

        # Function call with age parameter
        age = row['age']
        name_id = find_name_id_by_age(age)
        
        from pyspark.sql import Row
        data = row['metadata']

        # Function to convert Row to dictionary
        def row_to_dict(row):
            if isinstance(row, Row):
                return {k: row_to_dict(v) for k, v in row.asDict().items()}
            elif isinstance(row, list):
                return [row_to_dict(item) for item in row]
            else:
                return row

        # Convert data to dictionary and then to JSON
        data_dict = row_to_dict(data)
        json_data = json.dumps(data_dict, indent=4)

        # Prepare JSON data for hstore insertion
        json_data = json_data.replace('"', "'").replace('\n', '').replace(' ', '')


        ################################## fact_stat_h #######################################

        final_gp_insert_query_values_fsh.append( f""" (
        { f"'{hour_key}'"},{ f"'{day_key}'"},
        {row['dateid'] if row['dateid'] is not None else 0},
        {'hourid' if hourid is None else f"'{hourid}'"},
        {'hourid' if hourid is None else f"'{hourid}'"},
        {row['accountid'] if row['accountid'] is not None else 0},
        {row['appsiteid'] if row['appsiteid'] is not None else 0},
        {row['subappsiteid'] if row['subappsiteid'] is not None else 0},
        {row['countryid'] if row['countryid'] is not None else 0},
        {row['regionid'] if row['regionid'] is not None else 0},
        {row['cityid'] if row['cityid'] is not None else 0},
        {row['mobileoperatorid'] if row['mobileoperatorid'] is not None else 0},
        {row['devicebrandid'] if row['devicebrandid'] is not None else 0},
        {row['devicemodelid'] if row['devicemodelid'] is not None else 0},
        {row['deviceosid'] if row['deviceosid'] is not None else 0},
        {row['genderid'] if row['genderid'] not in ('None', None) else 0},
        {row['u_genderid'] if row['u_genderid'] not in ('None', None) else 0},
        {row['sum_netcost'] if row['sum_netcost'] is not None else 0},0,
        {row['sum_adfalconrevenue'] if row['sum_adfalconrevenue'] is not None else 0},
        {row['languageid'] if row['languageid'] is not None else 0},
        {row['environmenttype'] if row['environmenttype'] is not None else 0},
        {row['sum_netcost'] if row['sum_netcost'] is not None else 0},
        {row['sum_adjustednetcost'] if row['sum_adjustednetcost'] is not None else 0},
        {row['sum_billablecost'] if row['sum_billablecost'] is not None else 0},
        {row['sum_grosscost'] if row['sum_grosscost'] is not None else 0},
        {row['sum_agencyrevenue'] if row['sum_agencyrevenue'] is not None else 0},
        {row['adid'] if row['adid'] is not None else 0},0,0,0,0,0,0,0,0,
        {row['adcreativeunits'] if row['adcreativeunits'] is not None else 0},
        {row['accountid'] if row['accountid'] is not None else 0},
        {row['channelid'] if row['channelid'] is not None else 0},
        {row['campaignid'] if row['campaignid'] is not None else 0},
        {row['campaigntype'] if row['campaigntype'] is not None else 0},
        {row['campaigntype'] if row['campaigntype'] is not None else 0},
        {row['row_count'] if row['row_count'] is not None else 0},
        {row['campaignid'] if row['campaignid'] is not None else 0},
        {row['row_count'] if row['row_count'] is not None else 0},
        {row['advertiserid'] if row['advertiserid'] is not None else 0},
        {row['dateid'] if row['dateid'] is not None else 0},
        {row['adgroupid'] if row['adgroupid'] is not None else 0},
        {row['adcreativeunits'] if row['adcreativeunits'] is not None else 0},
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        {row['advassociationid'] if row['advassociationid'] is not None else 0},
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {name_id}, {name_id},
        '{row['EventCode'] if row['EventCode'] is not None else 0}',
        {row['deviceid_count'] if row['deviceid_count'] is not None else 0},0,
        {row['sum_adfalconrevenue'] if row['sum_adfalconrevenue'] is not None else 0},0,0
        )""")

        # If the batch size is reached or it's the last row, execute the batch
        if (j + 1) % batch_size == 0 or (j + 1) == len(joined_df_list):
            batch_values_fsh = ',\n'.join(final_gp_insert_query_values_fsh)
            final_query_fsh_batch = final_gp_insert_query_fields_fsh + batch_values_fsh
            # Print or execute the batch query
            try:
                if flag:
                    print(final_query_fsh_batch)
                gp_cursor.execute(final_query_fsh_batch)
                # print("All queries executed successfully")
            except Exception as e:
                print(f"Error executing query: {final_query_fsh_batch}")
                print(f"Error details: {str(e)}")
                gp_cursor.rollback()
            # Clear the list for the next batch
            final_gp_insert_query_values_fsh.clear()


    # Commit changes and close cursor and connection
    gp_connection.commit()
    gp_cursor.close()
    gp_connection.close()

# Stop SparkSession
spark.stop()

# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time
# Calculate the elapsed time
print(f"Elapsed time: {elapsed_time} seconds")
#####################################
print("\nPipeline completed successfully.")



