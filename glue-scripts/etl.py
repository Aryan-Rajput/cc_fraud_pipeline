import sys
import boto3
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_path = "s3://cc-fraud-pipeline-ar/bronze/transactions/"

def get_secret():
    client = boto3.client(
        service_name='secretsmanager',
        region_name='ap-southeast-1'
    )
    secret = client.get_secret_value(
        SecretId='cc-fraud/snowflake'
    )
    return json.loads(secret['SecretString'])

creds = get_secret()

SNOWFLAKE_OPTIONS = {
    "sfURL": creds["sfURL"],
    "sfUser": creds["sfUser"],
    "sfPassword": creds["sfPassword"],
    "sfDatabase": creds["sfDatabase"],
    "sfSchema": creds["sfSchema"],
    "sfWarehouse": creds["sfWarehouse"],
    "dbtable": "RAW_TRANSACTIONS"
}

# READING DATA

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(S3_path)
    
df = df.drop("_c0")

df = df.withColumn("load_timestamp", current_timestamp())

# WRITING DATA

df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .mode("append") \
    .save()

job.commit()

