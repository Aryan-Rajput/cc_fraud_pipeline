import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp

from env import SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE
from env import SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = sparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_path = "s3://cc-fraud-pipeline-ar//bronze//transactions/"

SNOWFLAKE_OPTIONS = {
    "sfURL": os.environ.get("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
    "sfUser": os.environ.get("SNOWFLAKE_USER"),
    "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE"),
    "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    "dbtable": "RAW_TRANSACTIONS"
}

# READING DATA

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(S3_path)

df = df.withColumn("load_timestamp", current_timestamp())

# WRITING DATA

df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .mode("append") \
    .save()

job.commit()

