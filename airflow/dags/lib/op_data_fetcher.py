import json
import requests
# from geopy.geocoders import Nominatim
from datetime import date
import os
from datetime import timezone
import datetime

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/BigData-AirPolution/DataLake/"

def fetch_data_from_op(**kwargs):
   data = query_data_from_ow()
   store_op_data(data)

def query_data_from_ow():
   dt = datetime.datetime.now(timezone.utc)

   utc_time = dt.replace(tzinfo=timezone.utc)
   start = str(int(utc_time.timestamp()))

   dt += datetime.timedelta(days=1)
   utc_time = dt.replace(tzinfo=timezone.utc)
   end = str(int(utc_time.timestamp()))

   # calling the Nominatim tool
   # loc = Nominatim(user_agent="GetLoc")

   # entering the location name
   # getLoc = loc.geocode("Paris")

   response = requests.get('http://api.openweathermap.org/data/2.5/air_pollution/history?lat=1.944658'
                           # +str(getLoc.latitude)+
                           '&lon=47.837765'
                           # +str(getLoc.longitude)+
                           '&start='+start+'&end='+end+'&appid=8adf3803f7d8f0aa352e19812fd097d8').json()
   return response

def store_op_data(data):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/OpenWeather/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)
   print("Writing here: ", TARGET_PATH)
   f = open(TARGET_PATH + "op.json", "w+")
   f.write(json.dumps(data, indent=4))

   import logging
   import boto3
   from botocore.exceptions import ClientError
   AWS_ACCESS_KEY_ID = 'AKIA5ISK54AT7URSUZVU'
   AWS_SECRET_ACCESS_KEY = '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'

   s3_client = boto3.client('s3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   try:
      response = s3_client.upload_file(TARGET_PATH + 'op.json', 'oussemadatalake',"raw/OpenWeather/" + current_day + "/" + 'op.json')
   except ClientError as e:
      logging.error(e)
