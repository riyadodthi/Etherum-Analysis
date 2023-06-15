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
        .appName("gg")\
        .getOrCreate()
    
    def trans_good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15: # Should be 15 fields
                return False
            int(fields[3]) # Block num field should be integer
            return True
        except:
            return False
        
    def contract_good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=6: # Should be 6 fields
                return False
            if fields[3]=='True' or fields[3]=='False': # is_erc20 column should be either True or False
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

    trans_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    
    trans_clean_lines = trans_lines.filter(trans_good_line)

    t_ds= trans_clean_lines.map(lambda l: (l.split(",")[11],l.split(",")[9])) #(date,gas_price)
     
    #Gas_price vs Time
    #Monthly count
    time_count = t_ds.map(lambda t: (time.strftime("%m-%Y",time.gmtime(int(t[0]))),1)) #(date,1)
    time_count = time_count.reduceByKey(add)   #(date,count)  
    
    #Sum price
    time_price =  t_ds.map(lambda t: (time.strftime("%m-%Y",time.gmtime(int(t[0]))),int(t[1]))) #(date,price)
    time_price = time_price.reduceByKey(add) #(date, agg_price)
    
    time_join = time_count.join(time_price) #(date,(count,price))
    
    # Avg gas price per month
    gasPrice_vs_time = time_join.map(lambda x: (x[0],x[1][1]/x[1][0]))
    print(gasPrice_vs_time.collect())
    

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'cw_gg1/q4gasprice.txt')
    my_result_object.put(Body=json.dumps(gasPrice_vs_time.collect()))

    
    spark.stop()
