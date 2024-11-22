import os
import boto3
import time
import botocore

SPLIT_APP_PATH = '/deps/pdftools-split/'
PDF2IMAGE_APP_PATH = 'deps/pdftools-pdf2image/' 
INPUT_FILE_PATH = 'input/input.pdf'


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
   CWD = os.path.dirname(__file__)
   # Init env
   s3_client = init_lambda_env()
   
   # Warm Up Event
   if "source" in event:
      lambda_logs("WARM UP EVENT")
      return
   
   # Clean response folder
   #clean_folder('/response')
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
      input_response = s3_client.download_file(input_bucket_name, input_bucket_key, INPUT_FILE_PATH)
   except Exception:
      lambda_logs(msg='something went wrong downloading the file from s3', level='error')
   
   lambda_logs(msg=f"File downloaded to {INPUT_FILE_PATH}")
   
   # Get action
   action = event['action']
   
   if action == 'split':
      os.chdir(SPLIT_APP_PATH)
      os.system(f"./pdftoolssplit {INPUT_FILE_PATH} /response/")
      # zip y subir
   elif action == 'pdf2image':
      os.chdir(PDF2IMAGE_APP_PATH)
      os.system(f"./pdftoolspdf2image {INPUT_FILE_PATH} /response/response.jpeg")
      
   s3_client.upload_file(Filename="/response/response.jpeg",Bucket=output_bucket_name,Key=output_bucket_key)
   
   
   
   

   
      
   
   
   
      
   
   
   
   
   