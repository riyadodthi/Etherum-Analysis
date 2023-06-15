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
        .appName("q1")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15: # Should be 15 fields
                return False
            int(fields[3]) # Block num field should be integer
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

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
 
    clean_lines = lines.filter(good_line)
    
    t_lines= clean_lines.map(lambda l: (l.split(",")[3],(l.split(",")[11],l.split(",")[7]))) #(block_id,(date,trans_val))
    
    #Number of transactions
    time_lines =  t_lines.map(lambda t: ((time.strftime("%m-%Y",time.gmtime(int(t[1][0])))),1)) #(date,1)
    count = time_lines.reduceByKey(add) #(date, countBy_key)
    
    
    #Avg transaction value
    month_avg = t_lines.map(lambda m: ((time.strftime("%m-%Y",time.gmtime(int(m[1][0])))),int(m[1][1]))) #(mm-yy, trans_val)
    total = month_avg.reduceByKey(add) #(mm-yy, total(trans_val))
    
    new = count.leftOuterJoin(total).collect() #(mm-yy, (countbykey,total(trans_val)))
    
    avg = spark.sparkContext.parallelize(new).map(lambda a: (a[0],a[1][1]/a[1][0])) #(mm-yy, avgByKey)
    
  
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'cw_' + date_time + '/q1_count.csv')
    my_result_object.put(Body=json.dumps(count.collect()))
    my_result_object = my_bucket_resource.Object(s3_bucket,'cw_' + date_time + '/q1_total.csv')
    my_result_object.put(Body=json.dumps(total.collect())) 
    my_result_object = my_bucket_resource.Object(s3_bucket,'cw_' + date_time + '/q1_avg.csv')
    my_result_object.put(Body=json.dumps(avg.collect()))    
    
    
    spark.stop()
