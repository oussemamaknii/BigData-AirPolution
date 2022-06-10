import os
from urllib.parse import to_bytes

import pandas as pd
from kafka import KafkaProducer
from time import sleep

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/BigData-AirPolution/DataLake/"

def convert_raw_to_formatted_gv(file_name, current_day):
   PATH = DATALAKE_ROOT_FOLDER + "raw/DataGouv/" + current_day + "/" + file_name
   FORMATTED_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/DataGouv/" + current_day + "/"
   if not os.path.exists(FORMATTED_FOLDER):
       os.makedirs(FORMATTED_FOLDER)
   df = pd.read_csv(PATH, sep=';')
   count = 0
   for index, row in df.iterrows():
      msg = f"{row['geo_point_2d']},{row['nom_dept']},{row['nom_com']},{row['insee_com']},{row['nom_station']},{row['code_station']},{row['typologie']},{row['influence']},{row['nom_poll']},{row['id_poll_ue']},{row['valeur']},{row['unite']},{row['date_debut']},{row['date_fin']},{row['statut_valid']},{row['code_epci']}"

      # print(msg)

      # send to Kafka
      producer = KafkaProducer(value_serializer=lambda v: to_bytes(v.encode('utf-8')),
                               bootstrap_servers='localhost:9092')

      producer.send('weather', msg)
      producer.flush()
      print(f'sending data to kafka, #{count}')

      count += 1

   parquet_file_name = file_name.replace(".csv", ".snappy.parquet")
   df.to_parquet(FORMATTED_FOLDER + parquet_file_name)

   import logging
   import boto3
   from botocore.exceptions import ClientError
   AWS_ACCESS_KEY_ID = 'AKIA5ISK54AT7URSUZVU'
   AWS_SECRET_ACCESS_KEY = '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'

   s3_client = boto3.client('s3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   try:
      response = s3_client.upload_file(FORMATTED_FOLDER + parquet_file_name , 'oussemadatalake',"formatted/DataGouv/" + current_day + "/" + parquet_file_name)
   except ClientError as e:
      logging.error(e)