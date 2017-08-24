from settings import DATABASES
# import MySQLdb
import pymysql


class MysqlClient(object):
    '''
    classdocse
    '''

    def __init__(self):
        '''
        Constructor
        '''
        # 数据库
        # self.__conn = MySQLdb.connect(host=DATABASES["alatting"].get("host"), user=DATABASES["alatting"].get("user"),
        #                               passwd=DATABASES["alatting"].get("password"), db=DATABASES["alatting"].get("db"),
        #                               charset="utf8")

        self.__conn = pymysql.connect(host=DATABASES["alatting"].get("host"), user=DATABASES["alatting"].get("user"),
                                      passwd=DATABASES["alatting"].get("password"), db=DATABASES["alatting"].get("db"),
                                      charset="utf8")

    def is_connected(self):
        """判断数据库连接是否断开"""
        if self.__conn is not None:
            try:
                self.__conn.ping()
                return True
            except:
                return False
        else:
            return False

    def reconnection(self):
        try:
            self.__conn.close()
            self.__conn = pymysql.connect(host=DATABASES["alatting"].get("host"),
                                          user=DATABASES["alatting"].get("user"),
                                          passwd=DATABASES["alatting"].get("password"),
                                          db=DATABASES["alatting"].get("db"),
                                          charset="utf8")
            self.__conn.autocommit(True)
        except Exception as e:
            pass

    @property
    def mysql_client(self):
        return self.__conn


if __name__ == '__main__':
    pass
