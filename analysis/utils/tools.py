# -*- coding: utf-8 -*-
'''
Created on Oct 19, 2016
@author: hw
'''
from datetime import timedelta
import pandas as pd


def get_current_date(format='%Y-%m-%d'):
    """
    获取当前日期
    ------------
    returns：
        date：YYYY-mm-dd
    """
    import time as tm
    return tm.strftime(format, tm.localtime(tm.time()))


def datetime_format(date, srcformat='%Y-%m-%d', tgtformat='%y%m%d'):
    """
    将一种格式的时间字符串转化为另外一种时间的字符串
    ------------
    returns：
        string：格式由tgtformat指定
    """
    import time as tm
    return tm.strftime(tgtformat, tm.strptime(date, srcformat))


def cal_datatime(date, format='%Y-%m-%d', days=0, hours=0, minutes=1):
    import datetime as tm
    minutes = timedelta(minutes=+minutes)
    hours = timedelta(hours=+hours)
    days = timedelta(days=+days)
    ret = tm.datetime.strptime(date, format) + minutes + hours + days
    return datetime_format(date=str(ret), srcformat='%Y-%m-%d %H:%M:%S', tgtformat=format)


def cal_pre_nums(src=[], tgt=[]):
    """
        计算列表的和，生成新的列表，新的数组的第n项是老的数组的前n项之和
        ------------
        returns：
            list：新的list
    """
    for index in range(len(src)):
        if index == 0:
            tgt.append(src[index])
        else:
            num = tgt[index - 1] + src[index]
            tgt.append(num)
    return tgt


def gen_time_serials(start='2016-01-01 00:00:00', end='2017-08-16 00:00:00', freq='D', date_format='%Y-%m-%d'):
    """
        生成一个start/end之间的时间序列
        start:开始日期
            format:%Y-%m-%d %H:%M:%S
        ------------
        returns：
            list：时间序列列表list
    """
    date_list = []
    time_serials = pd.date_range(start=start, end=end, freq=freq)
    for time in time_serials:
        date = datetime_format(date=str(time), srcformat='%Y-%m-%d %H:%M:%S', tgtformat=date_format)
        date_list.append(date)
    return date_list


if __name__ == '__main__':
    import time as tm

    date_list = gen_time_serials()
    print(date_list)
