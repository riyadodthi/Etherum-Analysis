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
    con_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
    trans_clean_lines = trans_lines.filter(trans_good_line)
    contract_clean_lines = con_lines.filter(contract_good_line)

    
    t_ds= trans_clean_lines.map(lambda l: (l.split(",")[6], (float(l.split(",")[8]), float(l.split(",")[7])))) #(to_add,(gas,value))
    con_ds = contract_clean_lines.map(lambda c: (c.split(',')[0], 1)) #(add, 1)
    
    join_ds=con_ds.join(t_ds) #(add, (1, (gas,value)))
  
    # Avg gas used
    gas_tot = join_ds.map(lambda t: ("Total gas",int(t[1][1][0]))) #(str,gas)
    totgas = gas_tot.reduceByKey(add) #(str, sum_gas)
    gas_count = gas_tot.count() 
                      
    avg_gas = totgas.map(lambda x: ("Avg gas used", x[1]/gas_count))
    print("Overall average gas used: ",avg_gas.collect())
    
    #most polpular contracts
    top_10 = spark.sparkContext.textFile("s3a://" + s3_bucket + "/cwq2/q2_top10.csv")

    pop_con = top_10.map(lambda x: x.split(",")) 
    pop_con = pop_con.map(lambda x: (x[0], x[1])) #(add, value)
    
    #Gas used by top 10 contracts
    top_10_gas_use = t_ds.join(pop_con)  #(add, (sum_val,(gas,value)))
    
    top_10_gas_use = top_10_gas_use.map(lambda x: (x[0],x[1][0][0])) #(add, gas)
    top_10_gas_use = top_10_gas_use.reduceByKey(add) #(add, tot_gas)
    print("Gas used by popular contracts:",top_10_gas_use.collect())
    
  
    spark.stop()
