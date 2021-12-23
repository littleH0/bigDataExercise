import sys
import csv
import os
import time
import jieba
from wordcloud import WordCloud
import matplotlib.pyplot as plt

base_file_path = ""
hour_of_two_month = [0 for i in range(24)]
danmu_len_list = list()
words = list()


def process_base_data(temp_day, file_name):
    reader = csv.reader(open(base_file_path + 'data/' + file_name, encoding='utf-8'))
    # month = int(file_name[4:6])
    # 每个月的数据
    count = 0
    for ri in reader:
        count += 1
        print(count, ri)
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
        if count == 50000:
            break
    # 把词存入文件
    with open(base_file_path + 'words1.txt', 'w+', encoding='utf-8') as f1:
        f1.write(" ".join(words))
    return temp_day


def draw(days):
    # 绘制每日图表
    # fig1 = plt.figure()
    # ax = fig1.add_axes([0, 0, 1, 1])
    # langs1 = [i for i in range(1, len(days))]
    # ax.bar(langs1, days[1:])
    # plt.show()

    # 绘制每时图表

    # 绘制弹幕长度图表

    #
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
    plt.show()
    # plt.draw()


if __name__ == '__main__':
    start_time = time.time()
    base_file_path = sys.argv[1]
    print(base_file_path)
    # [1] 读取csv文件并遍历
    danmu_len_list.clear()
    #sparkcontext
    day_of_11 = [0 for i in range(31)]
    day_of_11 = process_base_data(day_of_11, '202111.csv')
    print(day_of_11)
    print(danmu_len_list)
    print(len(danmu_len_list))
    time2 = time.time()
    print("计算数据耗时：", time2-start_time)
    # 去绘制
    draw(day_of_11)
    time3 = time.time()
    print("绘制图表耗时：", time3-time2)
    print("总耗时：", time3-start_time)

    # 绘制词云
    with open(base_file_path + 'words1.txt', 'r', encoding='utf-8') as f2:
        text = f2.read()
    wordcloud = WordCloud(font_path=base_file_path+"simsun.ttf").generate(text)
    image_produce = wordcloud.to_image()
    image_produce.show()
    print("绘制词云耗时：", time.time()-time3)

