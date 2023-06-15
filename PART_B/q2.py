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
        .appName("q2")\
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
    con_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
    
    trans_clean_lines = trans_lines.filter(trans_good_line)
    contract_clean_lines = con_lines.filter(contract_good_line)
    
    trans_ds =trans_clean_lines.map(lambda t: (t.split(',')[6], int(t.split(',')[7]))) #(to_add, value)
    trans_total = trans_ds.reduceByKey(add)
    con_ds = contract_clean_lines.map(lambda c: (c.split(',')[0],1)) #(add, 1)
    
    join_ds=con_ds.join(trans_total) #(add, (1, agg_value))
    add_value = join_ds.map(lambda x: (x[0], x[1][1])) 
    top_10 = add_value.takeOrdered(10,key=lambda x: -x[1]) # Top 10 values
    for record in top_10:
        print("Top_10 {}: {}".format(record[0],record[1]))
              
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'cwq2/q2_top10.csv')
    my_result_object.put(Body=json.dumps(top_10))
   
    
    spark.stop()