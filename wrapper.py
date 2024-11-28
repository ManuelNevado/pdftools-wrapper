import os
import boto3
import time
import botocore
from urllib.parse import unquote_plus
import uuid
import shutil

SPLIT_APP_PATH = '/app/split'
#PDF2IMAGE_APP_PATH = '/app/pdftools-pdf2image' 
PDF2IMAGE_APP_PATH = '/app/pdf2image'
TOPDFA_APP_PATH = '/app/topdfa'
INPUT_FILE_PATH = '/tmp/input/input.pdf'
OUTPUT_FILE_PATH = '/tmp/response/'

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
   
   CWD = os.path.dirname(__file__)
   
   #client_s3
   s3_client = boto3.client("s3")
   lambda_logs(msg=f"s3 client created. OBJ = {s3_client}")
   
   os.system('mkdir /tmp/input')
   os.system('mkdir /tmp/response')

   return s3_client
   

def handler(event, context=None):
   
   CWD = os.path.dirname(__file__)
   # Init env
   s3_client = init_lambda_env()
   
   # Warm Up Event
   if "source" in event:
      lambda_logs("WARM UP EVENT")
      return
   
   # Clean response folder
   #clean_folder('/response')
   
   # Initialize buckets params
   # Keys
   try:
      input_bucket_key = unquote_plus(event["inputKey"])
      output_bucket_key = input_bucket_key.split('.')[0]
      input_file_path = "/tmp/input/{}{}".format(uuid.uuid4(), input_bucket_key.replace("/", ""))
      lambda_logs(f"input_bucket_key: {input_bucket_key}")
      lambda_logs(f"output_bucket_key: {output_bucket_key}")
      lambda_logs(f"input_file_path: {input_file_path}")
   except Exception:
      lambda_logs('something went wrong loading the bucket keys')
   # Names
   try:
      input_bucket_name = event['inputBucketName']
      output_bucket_name = event['outputBucketName']
      lambda_logs(f"input_bucket_name: {input_bucket_name}")
      lambda_logs(f"output_bucket_name: {output_bucket_name}")
   except Exception:
     lambda_logs('something went wrong loading the bucket names')
   
   
   # Download file
   try:
      lambda_logs(f's3_client.download_file({input_bucket_name}, {input_bucket_key}, {input_file_path})')
      input_response = s3_client.download_file(input_bucket_name, input_bucket_key, input_file_path)
      lambda_logs(msg=f'Download Response: OK', level='low')
   except Exception:
      lambda_logs('something went wrong downloading the file from s3')
   
   # Get action
   action = event['action']
   
   if action == 'split':
      lambda_logs('split request')
      os.chdir(SPLIT_APP_PATH)
      os.system(f"./pdftoolssplit {input_file_path} {OUTPUT_FILE_PATH}")
      shutil.make_archive('/tmp/response', 'zip', OUTPUT_FILE_PATH)
      output_bucket_key+='.zip'
      try:
         s3_client.upload_file(Filename='/tmp/response.zip',Bucket=output_bucket_name,Key=output_bucket_key)
         lambda_logs('Upload OK')
      except:
         lambda_logs('Upload ERROR', level='error')
      # zip y subir
   elif action == 'pdf2image':
      lambda_logs('psd2image request')
      os.chdir(PDF2IMAGE_APP_PATH)
      os.system(f"./pdftoolspdf2imgsimple {input_file_path} {OUTPUT_FILE_PATH+'response.jpeg'}")
      output_bucket_key+='.jpeg'
      try:
         s3_client.upload_file(Filename=OUTPUT_FILE_PATH+'response.jpeg',Bucket=output_bucket_name,Key=output_bucket_key)
         lambda_logs('Upload OK')
      except:
         lambda_logs('Upload ERROR', level='error')
   elif action=='topdfa':
      lambda_logs('topdfa request')
      os.chdir(TOPDFA_APP_PATH)
      os.system(f"./pdftoolsvalidateconvert {input_file_path} {OUTPUT_FILE_PATH+'response.pdf'}")
      output_bucket_key+='.pdf'
      try:
         s3_client.upload_file(Filename=OUTPUT_FILE_PATH+'response.pdf',Bucket=output_bucket_name,Key=output_bucket_key)
         lambda_logs('Upload OK')
      except:
         lambda_logs('Upload ERROR', level='error')
   
   
   return output_bucket_key
   
   
   
   

   
      
   
   
   
      
   
   
   
   
   