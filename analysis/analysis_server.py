import logging
import tornado
import tornado.web
import tornado.ioloop
from tornado.options import define, options, parse_command_line
import log
from data_handler.data_processor import *
from data_handler.mysql_con import MysqlClient


# 初始化一些define
def init_define():
    define("flag", default=False, help="when system is first time,flag should be set True", type=bool)
    # log
    define("log_file_prefix", default="./log/analysis.log")
    define("log_rotate_mode", default='time')  # 轮询模式: time or size
    define("log_rotate_when", default='H')  # 单位: S / M / H / D / W0 - W6
    define("log_rotate_interval", default=24)  # 间隔: 24小时


# 初始化log
def init_logging():
    for handler in logging.getLogger().handlers:
        handler.setFormatter(log.LogFormatter())
        logging.getLogger().setLevel(logging.INFO)


# 初始化全局定时器
def init_periodic_server():
    # 定时更新所有房间的被禁言用户的状态，每秒刷新一次
    mysql_conn = MysqlClient()
    data_handler = DataProcessor(mysql_client=mysql_conn.mysql_client)
    if options.flag is True:
        logging.info("begin init data all days")
        data_handler.init_calc_data()
    tornado.ioloop.PeriodicCallback(data_handler.start_task, 30 * 1000).start()


# 入口主函数
def main():
    # 首先定义
    init_define()
    # tornado 的解析命令行
    parse_command_line()

    # 初始化log和tornado的基本设置
    init_logging()

    # 初始化定时器
    init_periodic_server()
    # 监听端口，开始主事件循环
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    # 主函数
    main()
