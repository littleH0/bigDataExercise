import sys
import re
import csv
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import *

start_time = time.time()

base_path = 'file:///home/hadoop/Desktop/dataProcess/'
# base_path = 'hdfs://master0:9000/dataProcess/'

hour_of_two_month = [0 for i in range(24)]
day_of_11 = [0 for i in range(31)]

# 
def process_base_data(temp_day, df):
    # 
    every_day_num = [0 for i in range(31)]
    every_hour_num = [0 for i in range(24)]
    df = df.withColumn('day',df.day.cast(IntegerType()))
    df = df.withColumn('hour',df.day.cast(IntegerType()))
    print(df.show(5)) 
    print("day==============")
    for i in range(1,31):
        every_day_num[i] = df.filter(df.day==i).count()
        print(i,every_day_num[i])
    print("hour=============")
    for j in range(0,24):
        every_hour_num[j] = df.filter(df.hour==j).count()
        print(j,every_day_num[j])
    print("len==============")
    # danmu_length = list()
    # df1 = df.withColumn('content', df['content'].length)
    # df1.groupBy('content').agg
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
    spark = SparkSession.builder.master(mode).appName("danmu_analyse").getOrCreate()
    df = spark.read.csv(base_path + '202111_min_format.csv', header=True)
    rdd = df.rdd
    rdd1 = rdd.map(lambda x: x[0].split(' '))
    rdd_day = rdd1.map(lambda x: x[0].split('-')[-1])
    rdd_hour = rdd1.map(lambda x: x[-1].split(':')[0])
    rdd_danmu_len = rdd.map(lambda x: len(str(x[1])))
    #
    # output = rdd_day.collect()
    # for item in output:
       # print(output)
    print("time:", time.time()-start_time)
    # 
    # ### process ###
    print("day==============")
    every_day_num = [0 for i in range(31)]

    schema = StructType([StructField('day', IntegerType())])
    day_df = spark.createDataFrame(rdd_day, schema)

    # day_df = rdd_day.map(line=>(line._1)).toDF("")
    print(day_df.show(5))
    # _day = rdd_day.collect()
    # for i in range(1,31):
        # every_day_num[i] = day_df.filter(df.day==i).count()
        # print(i,every_day_num[i])
    print("time:", time.time()-start_time)
    exit(0)
    #
    # print("hour==============")
    # every_hour_num = [0 for i in range(24)]
    # hour_df = rdd_hour.map{line=>(line._1)}.toDF("hour")
    # _hour = rdd_hour.collect()
    # for i in range(0,24):
        # every_hour_num[i] = hour_df.filter(df.hour==i).count()
        # print(i,every_day_num[i])
    # print("time:", time.time()-start_time)
    # exit()
    #
    # print("length==============")
    # danmu_map = {}
    # danmu_df = rdd_danmu_len.map{line=>(line._1)}.toDF("danmul") 
    # danmu_len = rdd_danmu_len.collect()
    # for dl in danmu_len:
        # if dl in danmu_map.keys():
            # danmu_map[dl] += 1
        # else:
            # danmu_map[dl] = 1
    # print("time:", time.time()-start_time)
    # print("before:", danmu_map)
    # sorted(danmu_map.items(),key = lambda x:x[1],reverse = True)
    # print("after:", danmu_map)
    #
    # print("time:", time.time()-start_time)
    # exit(0)

    # 
    day_of_11 = process_base_data(day_of_11, df)
    # print("11月份每天弹幕数量：",day_of_11)
    # sorted
    # print("11月份弹幕长度范围：",danmu_len_list)
    time2 = time.time()
    print("计算数据耗时：", time2-start_time)

