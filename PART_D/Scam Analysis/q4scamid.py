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
        .appName("q4a")\
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
    trans_ds =trans_clean_lines.map(lambda t: (t.split(',')[6], (int(t.split(',')[7]),(time.strftime("%m-%Y",time.gmtime(int(t.split(',')[11]))))))) #(to_add,(value,ts))
    print(trans_ds.take(5))
    
    scam_info = scam_lines.flatMap(lambda x: [(x['result'][scam]['addresses'],(x['result'][scam]['id'],x['result'][scam]['category'])) for scam in x['result']]) #([add1,add2],(id,cat))
    
    #(add1,([add1,add2],id,cat))
    scam_info = scam_info.flatMap(lambda s: ([(s[0][i],(s[0],s[1][0],s[1][1])) for i in range(len(s[0]))]))
    print(scam_info.take(5))
    
    #join
    join_ds = scam_info.join(trans_ds) 
    
    # map for profit rdd
    profit_rdd = join_ds.map(lambda x: (x[1][0][1], x[1][1][0])) #(ID,value)
    
    profit = profit_rdd.reduceByKey(add) #(ID, sum_val)
    max_profit =profit.takeOrdered(1,key=lambda x: -x[1]) 
    
    print("ID of the most lucrative scam is:  ",max_profit)
    
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)


    my_result_object = my_bucket_resource.Object(s3_bucket,'cw4scam/q4ID.txt')
    my_result_object.put(Body=json.dumps(max_profit))    
    
    
    spark.stop()