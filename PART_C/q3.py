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
        .appName("q3")\
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
    ds = clean_lines.map(lambda t: (t.split(',')[9], int(t.split(',')[12]))) #(miner,size)
    total = ds.reduceByKey(add).collect()
   
    top_10 = spark.sparkContext.parallelize(total).takeOrdered(10,key=lambda x: -x[1])
    for record in top_10:
        print("{}: {}".format(record[0],record[1]))
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'cwq3/q3_top10.csv')
    my_result_object.put(Body=json.dumps(top_10))    
    
    spark.stop()