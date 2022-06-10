from pyspark.sql import SQLContext, SparkSession
import os

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/BigData-AirPolution/DataLake/"

def combine_data(current_day):
   OP_PATH = DATALAKE_ROOT_FOLDER + "formatted/OpenWeather/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/analytics/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS)

   sparkSession = SparkSession.builder \
      .appName('examstat') \
      .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
      .config('spark.cassandra.connection.host', 'localhost') \
      .master('local[*]') \
      .getOrCreate()
   load_options = {"table": "weather", "keyspace": "bigd"}
   sqlContext = SQLContext(sparkSession)

   df0 = sqlContext.read.format('org.apache.spark.sql.cassandra').options(**load_options).load()
   df0.createTempView("gv_analytics")
   print(df0.show())

   # df_op = sqlContext.read.parquet(OP_PATH)
   # df_op.registerTempTable("op_analytics")
   # print(df_op.show())

   stats_df = sqlContext.sql("SELECT * FROM gv_analytics where  ")

   print(stats_df.show())
   stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")
   # df_op.write.save( USAGE_OUTPUT_FOLDER_STATS +"/op/" + "res.snappy.parquet", mode="overwrite")

   import logging
   import boto3
   import re
   from botocore.exceptions import ClientError
   AWS_ACCESS_KEY_ID = 'AKIA5ISK5g4AT7URSUZVU'
   AWS_SECRET_ACCESS_KEY = '/XSkkVMMXMqnTfuXfZgzdOOTNgicduPsWWxIyhSl'
   s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   for subdir, dirs, files in os.walk(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet"):
      for file in files:
         if re.match('^p', file):
            full_path = os.path.join(subdir, file)
            try:
               response = s3_client.upload_file(full_path , 'oussemadatalake',"usage/analytics/" + current_day + "/"+file)
            except ClientError as e:
               logging.error(e)