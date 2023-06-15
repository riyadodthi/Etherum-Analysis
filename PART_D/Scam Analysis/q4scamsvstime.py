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
import json
import ast




if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("q4b")\
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
    #Read file
    scam_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.json").map(lambda x: json.loads(x))
    trans_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    
    #Load data
    trans_clean_lines = trans_lines.filter(trans_good_line)
   
    #map data
    trans_ds =trans_clean_lines.map(lambda t: (t.split(',')[6], (int(t.split(',')[7]),(time.strftime("%m-%Y",time.gmtime(int(t.split(',')[11])))))))
    
    
    scam_info = scam_lines.flatMap(lambda x: [(x['result'][scam]['addresses'],(x['result'][scam]['id'],x['result'][scam]['category'],x['result'][scam]['status'])) for scam in x['result']])
    
    #rdd (add_11,([add11,add12],id1,cat1,status)
    scam_info1 = scam_info.flatMap(lambda s: ([(s[0][i],(s[0],s[1][0],s[1][1])) for i in range(len(s[0]))]))
    
    # join (add, (([addresses],id,cat,status),(value,ts)))
    join_ds = scam_info1.join(trans_ds)
    
    #map ((ts,cat),value)
    ts_rdd = join_ds.map(lambda x: ((x[1][0][2],x[1][1][1]), x[1][1][0]))
    print("Ts_rdd",ts_rdd.take(10))
    # reduce ((ts, cat), tot_value)
    ts_reduce = ts_rdd.reduceByKey(add)
    
    print("ts_total",ts_reduce.collect())
    
    # Map only scamming category 
    ts_cat = ts_reduce.map(lambda x: x).filter(lambda x:  x[0][0])
    print("Category",ts_cat.collect())
    
    ts_cat_agg = ts_cat.reduceByKey(add)
    print("cat_agg",ts_cat_agg.collect())
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'cw4scam/q4catime.txt')
    my_result_object.put(Body=json.dumps(ts_cat.collect()))
    
    spark.stop()