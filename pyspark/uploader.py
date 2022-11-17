import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
import json

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)
sc = spark.sparkContext
####################################
# Parameters
####################################

postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING api FILES")
print("######################################")

batch_size_arg = "?size=50"

api_url = 'https://random-data-api.com/api/company/random_company'

batched_api_url = api_url + batch_size_arg
tbl_name = api_url.split("/")[4]

r = requests.get(batched_api_url)
print(r.status_code)
d = json.loads(r.text)

rdd = sc.parallelize(d)
df = spark.read.json(rdd)
####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
    df.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", tbl_name)
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("append")
    .save()
)

