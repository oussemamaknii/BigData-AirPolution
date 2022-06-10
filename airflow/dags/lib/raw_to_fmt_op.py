import os
import json
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/BigData-AirPolution/DataLake/"

def convert_raw_to_formatted_op(file_name, current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/OpenWeather/" + current_day + "/" + file_name
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/OpenWeather/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   data = json.load(open(RATING_PATH))
   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   dataa = []
   columns = ['lon','lat']
   for key in data['list'][1]['components'].keys():
      columns.append(key)
   for i in range(len(data['list'])) :
      d = [data['coord']['lon'],data['coord']['lat']]
      for j in data['list'][i]['components'] :
         d.append(data['list'][i]['components'][j])
      d.append(data['list'][i]['dt'])
      dataa.append(d)
   columns.append('dt')
   final_df = pd.DataFrame(dataa,columns=columns)
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)

   import logging
   import boto3
   from botocore.exceptions import ClientError
   AWS_ACCESS_KEY_ID = 'AKIA5ISK54AT7URSUZVU'
   AWS_SECRET_ACCESS_KEY = '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'

   s3_client = boto3.client('s3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   try:
      response = s3_client.upload_file(FORMATTED_RATING_FOLDER + parquet_file_name , 'oussemadatalake',"formatted/OpenWeather/" + current_day + "/" + parquet_file_name)
   except ClientError as e:
      logging.error(e)