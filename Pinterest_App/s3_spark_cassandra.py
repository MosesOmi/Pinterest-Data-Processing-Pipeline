# %%

import findspark

import multiprocessing

import pyspark

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import regexp_replace
from sqlalchemy import table

findspark.init()

os.environ["PYSPARK_SUBMIT_ARGS"]="--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 pyspark-shell"

cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("TestApp")
    .set("spark.eventLog.enabled", False)
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    .setIfMissing("spark.executor.memory", "1g")
)

print(cfg.get("spark.executor.memory"))
print(cfg.toDebugString())

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

access_key_id="AKIA2VICQPQOAMTCIDI4"
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key_id)
secret_access_key="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"
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

df = spark.read.json("s3a://pinterest-data-d408f649-ee2f-464f-8572-0fba5a860d8f/*.json")

df1 = df.drop("downloaded")
df2 = df1.sort("category")
df3 = df2.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

df3.printSchema()
df3.show(truncate=True)

df3.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="pin_data", keyspace="pinterest").save()
# model_table = spark.read.format("org.apache.spark.cassandra").options(table="pinterest")
spark.stop()
# %%
