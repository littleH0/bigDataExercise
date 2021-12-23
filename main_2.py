import sys
import re
import csv
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_extract

start_time = time.time()

# base_path = 'file:///home/hadoop/Desktop/dataProcess/'
base_path = 'hdfs://master0:9000/dataProcess/'

# 
def process_base_data(df): 
    every_day_num = [0 for i in range(31)]
    every_hour_num = [0 for i in range(24)]
    df = df.withColumn('day',df.day.cast(IntegerType()))
    df = df.withColumn('hour',df.hour.cast(IntegerType()))
    print(df.show(5)) 
    print("day==============")
    for i in range(1,31):
        every_day_num[i] = df.filter(df.day==i).count()
        print(i,every_day_num[i])
    print("hour=============")
    for j in range(0,24):
        every_hour_num[j] = df.filter(df.hour==j).count()
        print(j,every_hour_num[j])
    print("len==============")
    df1 = df.withColumn('content_length',functions.length(df.content))
    df2 = df1.groupBy("content_length").agg({"content_length": "sum"}).withColumnRenamed("sum(content_length)", "length_count")
    df3 = df2.orderBy('length_count', ascending=False)
    print(df3.show())
    # 
    print(every_day_num)
    print(every_hour_num)
    print("total time:",time.time()-start_time)
    exit(0)


if __name__ == '__main__':
    mode = "spark://master0:7077"    
    param = sys.argv[1]
    if param == '1':
        mode = "local"
    elif param == '2':
        mode = "local[*]"
    print("\n",mode, "\n")
    #  
    # spark = SparkSession.builder.master(mode).config("spark.executor.memory", "2g").appName("danmu_analyse").getOrCreate()
    spark = SparkSession.builder.master(mode).appName("danmu_analyse").getOrCreate()
    df = spark.read.csv(base_path + '202111_30280839_new.csv', header=True)
    # 
    day_of_11 = process_base_data(df)
    time2 = time.time()
    print("计算数据耗时：", time2-start_time)

