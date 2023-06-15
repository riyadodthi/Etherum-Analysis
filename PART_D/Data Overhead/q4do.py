import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from operator import add
from functools import partial
from operator import itemgetter




if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("q4b")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=19: # Should be 19 fields
                return False
            int(fields[0]) # Block num field should be integer
            return True
        except:
            return False
        
  

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
   
    clean_lines = lines.filter(good_line)
    
    # map ("Useless cols',(((len(string)-2)/2)))
    unnecessary_cols = clean_lines.map(lambda x: ("Total unnecessary data consumed: ",((float((len(x.split(',')[4])-2)/2))+(((len(x.split(',')[5])-2)/2))+(((len(x.split(',')[6])-2)/2))+(((len(x.split(',')[7])-2)/2))+(((len(x.split(',')[8])-2)/2))))) #(""Useless cols", logs_bloom + sha3_uncles+ transactions_root+ state_root+ receipts_root)
    
    #print(unnecessary_cols.take(10))
    
    unnecessary_sum = unnecessary_cols.reduceByKey(add) # aggregate data of all rows 
    print("Total unnecessary data consumed (Bytes): ", unnecessary_sum.collect())
   
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'cw4DO/q4_data.txt')
    my_result_object.put(Body=json.dumps(unnecessary_sum.collect()))

    
    spark.stop()