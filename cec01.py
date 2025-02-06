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
f_batchid = 202502031205
f_b_year = str(f_batchid)[0:4]
f_b_year_month = str(f_batchid)[0:6]
f_b_month = str(f_batchid)[4:6]
f_b_day = str(f_batchid)[6:8]
f_b_hour = str(f_batchid)[8:10]
f_b_hourid = str(f_batchid)[0:10]
f_b_min = '05'
f_b_month = str(int(f_b_month))
f_b_day = str(int(f_b_day))

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

# hdfs_path = f"{hdfs_url}/dw/kafka/topics/richmedia_ingest_logs/batches/{f_b_year_month}/batchid={f_batchid}/hourid={f_b_hourid}/minute=00/*"
# hdfs_path="C:\\Users\\rishu\\Downloads\\2024042508\\hourid=2024042508\\minute=00*"
hdfs_path = "hdfs://172.22.137.155:8020/temp/kafka02/topics_test02/richmedia_ingest_logs/batches/202502/batchid=202502061005/hourid=2025020610/minute=00/*"


print("Starting pipeline...")
print("Start time: ",start_time)
# # SQL query
# sql_query = """
# SELECT
#     IFNULL(acu.Id, 0) AS adcreativeunits,
#     IFNULL(acu.Adid, 0) AS adid,
#     IFNULL(a.AdGroupId, 0) AS adgroup_id,
#     IFNULL(ag.CampaignId, 0) AS campaign_id,
#     IFNULL(c.AccountId, 0) AS account_id,
#     IFNULL(c.TypeId, 0) AS type_id,
#     IFNULL(c.advertiserid, 0) AS advertiserid,
#     IFNULL(c.AssociationAdvId, 0) AS association_adv_id
# FROM
#     adcreativeunits acu
# INNER JOIN
#     ads a ON acu.Adid = a.Id
# INNER JOIN
#     adgroups ag ON a.AdGroupId = ag.Id
# INNER JOIN
#     campaigns c ON ag.CampaignId = c.Id
# WHERE
#     c.EndDate >= '2024-04-01' or (c.EndDate is null and a.StatusId = 10)
# """
# # Read data from MySQL
# try:
#     conn = mysql.connector.connect(**config.mysql_config)
#     cursor = conn.cursor()
#     cursor.execute(sql_query)
#     rows = cursor.fetchall()
#     df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
#     df.fillna(0, inplace=True)
#     if (flag) : print(df)
# except mysql.connector.Error as err:
#     if (flag) : print("Error:", err)
# finally:
#     if 'conn' in locals() and conn.is_connected():
#         cursor.close()
#         conn.close()
# print("MySQL data loaded to the dataframe successfully")
spark_df = spark.read.parquet(hdfs_path)


# spark_df.show(1)
# # spark_df.printSchema()
# exit()

# df = spark_df.withColumn("adcreativeunitsdata", explode(col("metadata.adcreativeunitsdata")))

# # Selecting the necessary fields
# df = df.select(
#     col("adcreativeunitsformat.key").alias("key1"),
#     col("adcreativeunitsdata.key").alias("key"),
#     col("adcreativeunitsdata.value.adid").alias("adid"),
#     col("adcreativeunitsdata.value.adgroupid").alias("adgroupid"),
#     col("adcreativeunitsdata.value.campaignid").alias("campaignid"),
#     col("adcreativeunitsdata.value.accountid").alias("accountid"),
#     col("adcreativeunitsdata.value.campaigntype").alias("campaigntype"),
#     col("adcreativeunitsdata.value.advertiserid").alias("advertiserid"),
#     col("adcreativeunitsdata.value.advassociationid").alias("advassociationid"),
#     col("adcreativeunitsdata.value.retail").alias("retail")
# )

# df.show(1)
# spark_df.show(1)
spark_df.printSchema()
# # exit()

from pyspark.sql.functions import col, sum, count, explode

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
print(spark_df.columns)
# Cast numeric columns to double
numeric_cols = [
    # "Appsiterevenue",
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
    "adid",  # Now explicitly available
    "adgroupid",
    "campaignid",
    "accountid",
    "campaigntype",
    "advertiserid",
    "advassociationid",
    "EventCode",
    "adcreativeunits"
).agg(
    sum(col("AdFalconRevenue")).alias("sum_adfalconrevenue"),
    sum(col("BillableCost")).alias("sum_billablecost"),
    sum(col("AdjustedNetCost")).alias("sum_adjustednetcost"),
    sum(col("NetCost")).alias("sum_netcost"),
    sum(col("GrossCost")).alias("sum_grosscost"),
    sum(col("AgencyRevenue")).alias("sum_agencyrevenue"),
    count("RequestId").alias("row_count"),
    count("DeviceId").alias("deviceid_count")
)
print("Spark data loaded to the dataframe successfully")
if (flag) : agg_df.show(2)

# exit()
# adcreative_ids = []
# adcreativeunitsformat_keys = []
# for  row in agg_df.collect():
#     key = row['adcreativeunitsformat'][0]['key']
#     adcreative_ids.append(key)  # Append the key to adcreative_ids
#     # Assuming you have a column named 'adcreative_id' in the DataFrame, replace it with        the correct column     name
#     # adcreativeunitsformat_keys.append((key, row['adcreative_id']))
#     if (flag) : print("adcreativeunitsformat_Key:", key)

# if (flag) : print("adcreative_ids : ", adcreative_ids)

# # Add adcreative ID column to DataFrame 406028
# if environment == "prod" :
#     get_adcreative_id_udf = udf(lambda key: key, IntegerType())
# if environment == "dev":
#     get_adcreative_id_udf = udf(lambda key: 406028 if key == 401646 else key, IntegerType())

# agg_df = agg_df.withColumn("adcreative_id", get_adcreative_id_udf(col("adcreativeunitsformat")[0]["key"]))

# # Show DataFrame after adding adcreative ID
# if (flag) : agg_df.show(20) # will comment
# print("Spark DataFrame created successfully")






# ############################################
# # Convert pandas DataFrame to Spark DataFrame
# pandas_df = spark.createDataFrame(df)

# Join the two DataFrames on 'adcreative_id'
# joined_df = pandas_df.join(agg_df, agg_df['adcreative_id'] == pandas_df['adcreativeunits'])
joined_df = agg_df

# Show the joined DataFrame
if (flag) : joined_df.show(10)
agg_count= joined_df.count()
if (flag) : print("agg_count: ",agg_count)
if (agg_count) < 1 : 
    print("No data available.. exiting")
    spark.stop()
    sys.exit(1)


# Assuming joined_df is the joined DataFrame

# Iterate over the rows of the DataFrame
# for row in joined_df.collect():
    # Access the value of the 'adcreativeunits' column for each row
    # adcreativeunits_value = row['adcreativeunits']
    # print("adcreativeunits value for this row:", adcreativeunits_value)

# Initialize an empty list to store the SQL insert queries
# gp_table_name = "fact_stat_d"

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


# event fields
event_field = ''
if eventCode == '000imp':
    event_field = 'impressions'
elif eventCode == '000clk':
    event_field = 'clicks'
gp_cursor.execute("SELECT column_name FROM information_schema.columns")
                #    WHERE table_name = 'fact_stat_d'")
column_names = [row[0] for row in gp_cursor.fetchall()]


# Set to keep track of generated keys
generated_keys = set()


# batch size for generated
batch_size = 5000

final_gp_insert_query_values_fsh = []
tablename= "fact_stat_customevent_h"

final_gp_insert_query_fields_fsh = f"""INSERT INTO {tablename} (hour_key,      dateid, hourid,     requesthourid, advaccountid,  appsiteid, subappsiteid, countryid,       regionid, cityid, mobileoperatorid,   devicebrandid, devicemodelid, deviceosid, genderid,      u_genderid, spend, appsiterevenue, adfalconrevenue,   languageid, environmenttype, netcost,     adjustednetcost, billablecost, grosscost, agencyrevenue, Adid,  platformfee, datafee,      thirdpartyfee, requests, requests_d, unfilledrequests, unfilledrequests_d,            connectiontype, creativeunitgroupid, pubaccountid, channelid, CampaignId, campaigntype,        adtypegroupid,  custom_event_count, dealid, wins, advertiserid, requestdateid, adgroupid,       adcreativeunitid, keywordgroupid,   u_keywordgroupid, requests_dcr, requests_cr, pageviews,        vcreativeviews, vstart, vfirstquartile, vmidpoint,    vthirdquartile, vcomplete,    conversions, requests_ad, requests_ag, requests_ca, requests_dad, requests_dag,          requests_dca, totaldataproviderrevenue, advassociationid, conv_pr, conv_pr_ct, conv_pr_vt,     conv_ot,     conv_ot_ct, conv_ot_vt, conv_pr_rev, conv_pr_ct_rev, conv_pr_vt_rev,    conv_ot_rev, agegroupid, u_agegroupid, event_name, unique_event_count) VALUES
""" 

joined_df_list = joined_df.collect()
# Iterate over the DataFrame rows
for j, row in  enumerate(joined_df_list):
    # Generate unique UUIDs for day_key, week_key, and month_key
    hour_key = str(uuid.uuid4())
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



    ################################## fact_stat_h #######################################

    final_gp_insert_query_values_fsh.append( f""" (
    { f"'{hour_key}'"},
    {row['dateid'] if row['dateid'] is not None else 0},
    {'hourid' if hourid is None else f"'{hourid}'"},
    {'hourid' if hourid is None else f"'{hourid}'"},
    {row['account_id'] if row['account_id'] is not None else 0},
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
    {row['sum_spend'] if row['sum_spend'] is not None else 0},
    {row['sum_appsiterevenue'] if row['sum_appsiterevenue'] is not None else 0},
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
    {row['account_id'] if row['account_id'] is not None else 0},
    {row['channelid'] if row['channelid'] is not None else 0},
    {row['campaign_id'] if row['campaign_id'] is not None else 0},
    {row['type_id'] if row['type_id'] is not None else 0},
    {row['type_id'] if row['type_id'] is not None else 0},
    {row['row_count'] if row['row_count'] is not None else 0},
    {row['campaign_id'] if row['campaign_id'] is not None else 0},
    {row['row_count'] if row['row_count'] is not None else 0},
    {row['advertiserid'] if row['advertiserid'] is not None else 0},
    {row['dateid'] if row['dateid'] is not None else 0},
    {row['adgroup_id'] if row['adgroup_id'] is not None else 0},
    {row['adcreativeunits'] if row['adcreativeunits'] is not None else 0},
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    {row['association_adv_id'] if row['association_adv_id'] is not None else 0},
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {name_id}, {name_id}, {EventCode}, {deviceid_count})"""
    )

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
print(f"Elapsed time: {elapsed_time} seconds")
# Simulate a failure for demonstration
# raise Exception("Simulated pipeline failure")
print("Pipeline completed successfully.")

#####################################

spark.stop()


