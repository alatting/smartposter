# coding=utf-8
"""
author : hw
date: 2017-08-24
description: 日志格式化
"""
import time

import datetime
import tornado.log


# 日志输出格式化
class LogFormatter(tornado.log.LogFormatter):
    def __init__(self):
        super(LogFormatter, self).__init__(
            fmt='%(color)s[%(asctime)s] [%(levelname)s] [%(filename)s:%(funcName)s:%(lineno)d] %(message)s%(end_color)s',
            datefmt=None  # '%Y-%m-%d %H:%M:%S'
        )

    # 支持毫秒显示
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)

        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            s = str(datetime.datetime.now())
        return s
