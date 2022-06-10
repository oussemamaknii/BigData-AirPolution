import os
from datetime import date
import logging
import boto3
from botocore.exceptions import ClientError
import os
import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/BigData-AirPolution/DataLake/"

def fetch_data_from_gv(**kwargs):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/DataGouv/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)

   url = 'https://www.data.gouv.fr/fr/datasets/r/ce203343-6ed9-4fd3-b310-e553ae437f6d'
   r = requests.get(url, allow_redirects=True)
   open(TARGET_PATH + 'data.csv', 'wb').write(r.content)


   AWS_ACCESS_KEY_ID = 'AKIA5ISK54AT7URSUZVU'
   AWS_SECRET_ACCESS_KEY = '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'

   s3_client = boto3.client('s3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   try:
      response = s3_client.upload_file(TARGET_PATH + 'data.csv', 'oussemadatalake',"raw/DataGouv/" + current_day + "/" + 'data.csv')
   except ClientError as e:
      logging.error(e)
