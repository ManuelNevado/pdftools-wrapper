import os
import boto3
from datetime import datetime
from fileinput import filename
import json
import time
import uuid
import logging
import boto3
import botocore
from urllib.parse import unquote_plus
from collections import Counter
from statistics import mean

SPLIT_APP_PATH = '/deps/pdftools-split/pdftoolssplit'
PDF2IMAGE_APP_PATH = 'deps/pdftools-pdf2image/pdftoolspdf2imagesimple' 


def lambda_logs(msg, level='low'):
   tick = time.time()
   header = str()
   if level == 'low':
      header += "INFO"
   elif level == 'med':
      pass
   elif level == 'error':
      header += "ERROR"
   else:
      pass
   
   print(f"(~{tick}~) :::: {header} :::: {msg}")

def clean_folder(path):
   # *********************************************
    # Delete all files in folder {path}
    # if the path contains a folder it invokes it again
    # *********************************************
    
    lambda_logs(f'cleaning folder: {path}')
    elements  = os.listdir(path)
    
    for element in elements:
        element_path = os.path.join(path, element)
        if os.path.isdir(element_path):
            clean_folder(element_path)
        else:
            os.remove(element_path)

def init_lambda_env():
   
   #client_s3
   s3_client = boto3.client("s3")
   lambda_logs(msg=f"s3 client created. OBJ = {s3_client}")
   
   return s3_client
   

def handler(event, context=None):
   
   # Init env
   s3_client = init_lambda_env()
   
   # Warm Up Event
   if "source" in event:
      lambda_logs("WARM UP EVENT")
      return
   
   # Clean response folder
   clean_folder('/response')
   lambda_logs("response folder clean")
   
   # Initialize buckets params
   # Keys
   try:
      input_bucket_key = event['inputKey']
      output_bucket_key = event['outputKey']
   except Exception:
      lambda_logs(msg='something went wrong loading the bucket keys', level='error')
   # Names
   try:
      input_bucket_name = event['inputName']
      output_bucket_name = event['outputName']
   except Exception:
      lambda_logs(msg='something went wrong loading the bucket names', level='error')
   
   
   # Download file
   try:
      input_file = s3_client.download_file(input_bucket_name, input_bucket_key, '/input/input.pdf')
   except Exception:
      lambda_logs(msg='something went wrong downloading the file from s3', level='error')
   
   lambda_logs(msg="File downloaded to /input/input.pdf")
   
   
   
      
   
   
   
      
   
   
   
   
   