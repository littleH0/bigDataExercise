import sys
import re
import csv
import os
import time
import jieba
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_extract

start_time = time.time()

# base_path = 'file:///home/hadoop/Desktop/dataProcess/'
base_path = 'hdfs://master0:9000/dataProcess/'

base_file_path = ""
hour_of_two_month = [0 for i in range(24)]
danmu_len_list = list()
words = list()
day_of_11 = [0 for i in range(31)]

#
spark = SparkSession.builder.master("local").appName("danmu_analyse").getOrCreate()
df = spark.read.csv(base_path + '202111.csv', header=False)

# sc = SparkContext()
# sqlContext = SQLContext(sc)
# df = sqlContext.read.format('com.databricks.spark.csv').options(header='false',   inferschema='true').load(base_path + '202111.csv')

# 
def process_base_data(temp_day):
    total_l = 1000    # 1800000
    df1 = df.take(total_l)
    count = 0
    pattern = re.compile(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})")
    extrect_str=pattern
    df.select(regexp_extract(col("_c0"),extrect_str)).show(5,False)
    print(df) 
    exit(0)
    for ri in df1:
        if pattern.search(ri[0]) is None:
            continue
        # [2] 统计两个月每日的弹幕数量并绘制图表
        if int(ri[0].split(' ')[0].split('-')[1]) != 11:
            continue
        _day = int(ri[0].split(' ')[0].split('-')[2])
        temp_day[_day] += 1

        # [3]每时发送弹幕做图标，分析用户经常什么时候看视频
        _hour = int(ri[0].split(' ')[1].split(':')[0])
        hour_of_two_month[_hour] += 1

        # [4] 用户发送弹幕的长度分析，绘制图表  得出结论，大家发多长弹幕为主
        danmu_l = len(ri[1])
        if danmu_l not in danmu_len_list:
            danmu_len_list.append(danmu_l)

        # [5] 绘制词云
        temp_wl = jieba.cut(ri[1])
        for wi in temp_wl:
            if wi in words:
                continue
            words.append(wi)

        # [end] 弹幕数量过多，只截取一部分比如 200,0000分析
        count += 1
        if count % 5000 == 0:
            print("进度：",count,"/",total_l)
            if count == total_l:
                break  
    # 把词存入文件
    # with open(base_path + 'words1.txt', 'w+', encoding='utf-8') as f1:
        # f1.write(" ".join(words))
    return temp_day


def draw(days):
    x1 = [i for i in range(0, len(hour_of_two_month))]
    x2 = [i for i in range(0, len(danmu_len_list))]
    x3 = [i for i in range(1, len(days))]
    fig = plt.figure()
    axes1 = fig.add_subplot(2, 2, 1)
    axes2 = fig.add_subplot(2, 2, 2)
    axes3 = fig.add_subplot(2, 1, 2)
    axes1.plot(x1, hour_of_two_month, 'ro--')
    axes2.plot(x2, danmu_len_list, 'gv--')
    axes3.plot(x3, days[1:], 'b*--')
    # plt.show()
    # plt.draw()


if __name__ == '__main__':
    print(df.show(5))
    # [1] 读取csv文件并遍历
    danmu_len_list.clear()
    # 
    day_of_11 = process_base_data(day_of_11)
    print("11月份每天弹幕数量：",day_of_11)
    # sorted
    print("11月份弹幕长度范围：",danmu_len_list)
    time2 = time.time()
    print("计算数据耗时：", time2-start_time)
    # 去绘制
    draw(day_of_11)
    time3 = time.time()
    print("绘制图表耗时：", time3-time2)
    print("总耗时：", time3-start_time)
    exit(0)
    # 绘制词云
    print("暂时不绘制词云")
    with open(base_path + 'words1.txt', 'r', encoding='utf-8') as f2:
        text = f2.read()
    wordcloud = WordCloud(font_path=base_file_path+"simsun.ttf").generate(text)
    image_produce = wordcloud.to_image()
    image_produce.show()
    print("绘制词云耗时：", time.time()-time3)

