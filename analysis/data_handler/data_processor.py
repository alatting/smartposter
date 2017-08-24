# -*- utf-8 -*-
"""
author : hw
date: 2017-08-24
"""
import time
import logging
from sqlalchemy.orm import sessionmaker
from utils import tools
import pandas as pd
from data_handler.mysql_con import MysqlClient
from sqlalchemy import create_engine
import json
from data.data_model import PageView, BrowserView, SystemView, ScreenView, DeviceView, Product, Card, NormalAction, \
    Coupon, ShareAction, WebsiteSummary, WebsiteUserSummary, WebsitePosterSummary, EventAction, SourceView, \
    PagesDetailView
from settings import address_list,DATABASES


class DataProcessor(object):
    def __init__(self, mysql_client):
        '''
        数据库
        '''
        self.__mysql_conn = mysql_client
        self.__current_day = tools.get_current_date('%Y-%m-%d')
        str_database = "mysql://{user}:{password}@{host}/{db}".format(
              user=DATABASES["alatting"].get("user","root"),
              password=DATABASES["alatting"].get("password"),
              host=DATABASES["alatting"].get("host"),
              db=DATABASES["alatting"].get("db")
          )
        print(str_database)
        engine = create_engine(str_database,
                               encoding='utf-8', echo=False)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    # 每天日切后开始同步统计数据
    def start_task(self):
        try:
            curr_day = tools.get_current_date('%Y-%m-%d')
            self.init_calc_data(curr_day)
        except Exception as e:
            pass

    # 统计海报浏览数据统计表PV,UV,IP,VV
    def calc_poster_page_view(self, date=None):
        # 独立IP
        if date is None:
            str_sql = "SELECT posterid as 'poster_id', DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(ip_id,0)) as 'ip' " \
                      " from analysis_period  GROUP BY DATE_FORMAT(start_at,'%Y-%m-%d'),posterid ;"
        else:
            str_sql = "SELECT posterid as 'poster_id', DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(ip_id,0)) as 'ip' " \
                      " from analysis_period WHERE 1=1 AND  DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}' GROUP BY DATE_FORMAT(start_at,'%Y-%m-%d'),posterid ;".format(
                date=date)
        # df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date'])
        df_ip = pd.read_sql(str_sql, self.__mysql_conn)
        # 独立UV
        if date is None:
            str_sql = "SELECT posterid as 'poster_id', DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(creator_id,0)) as 'uv' " \
                      "from analysis_period  GROUP BY DATE_FORMAT(start_at,'%Y-%m-%d'),posterid ;"
        else:
            str_sql = "SELECT posterid as 'poster_id', DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(creator_id,0)) as 'uv' " \
                      "from analysis_period  WHERE DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}' GROUP BY DATE_FORMAT(start_at,'%Y-%m-%d'),posterid ;".format(
                date=date)
        # df_uv = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date'])
        df_uv = pd.read_sql(str_sql, self.__mysql_conn)
        # 独立PV
        if date is None:
            str_sql = "select DATE_FORMAT(b.created_at,'%Y-%m-%d') as 'date',a.posterid as 'poster_id',COUNT(a.id) as 'pv' " \
                      "from analysis_period as a, analysis_logs as b where a.id = b.period_id GROUP BY a.posterid,DATE_FORMAT(b.created_at,'%Y-%m-%d')" \
                      " ORDER BY DATE_FORMAT(b.created_at,'%Y-%m-%d');"
        else:
            str_sql = "select DATE_FORMAT(b.created_at,'%Y-%m-%d') as 'date',a.posterid as 'poster_id',COUNT(a.id) as 'pv' " \
                      "from analysis_period as a, analysis_logs as b where DATE_FORMAT(b.created_at,'%Y-%m-%d') = '{date}' " \
                      "AND a.id = b.period_id GROUP BY a.posterid,DATE_FORMAT(b.created_at,'%Y-%m-%d')" \
                      " ORDER BY DATE_FORMAT(b.created_at,'%Y-%m-%d');".format(date=date)
        # df_pv = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date'])
        df_pv = pd.read_sql(str_sql, self.__mysql_conn)
        # VV
        if date is None:
            str_sql = "SELECT DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',posterid as 'poster_id',count(id) as'vv' " \
                      "from analysis_period GROUP BY posterid,DATE_FORMAT(start_at,'%Y-%m-%d')"
        else:
            str_sql = "SELECT DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',posterid as 'poster_id',count(id) as'vv' " \
                      "FROM analysis_period WHERE DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}' GROUP BY posterid,DATE_FORMAT(start_at,'%Y-%m-%d')".format(
                date=date)
        # df_vv = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date'])
        df_vv = pd.read_sql(str_sql, self.__mysql_conn)
        # 新用户数
        if date is None:
            str_sql = "select posterid as 'poster_id',DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(creator_id,0)) as 'new_user'  " \
                      "from analysis_period WHERE is_new = 1 GROUP BY posterid,DATE_FORMAT(start_at,'%Y-%m-%d') ORDER BY posterid, DATE_FORMAT(start_at,'%Y-%m-%d');"
        else:
            str_sql = "select posterid as 'poster_id',DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(creator_id,0)) as 'new_user'  " \
                      "from analysis_period WHERE DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}' AND is_new = 1 " \
                      "GROUP BY posterid,DATE_FORMAT(start_at,'%Y-%m-%d') ORDER BY posterid, DATE_FORMAT(start_at,'%Y-%m-%d');".format(
                date=date)
        # df_new_user = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date'])
        df_new_user = pd.read_sql(str_sql, self.__mysql_conn)
        # 老用户数
        if date is None:
            str_sql = "select posterid as 'poster_id',DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(creator_id,0)) as 'old_user' " \
                      "from analysis_period WHERE is_new = 0 GROUP BY posterid,DATE_FORMAT(start_at,'%Y-%m-%d') ORDER BY posterid, DATE_FORMAT(start_at,'%Y-%m-%d');"
        else:
            str_sql = "select posterid as 'poster_id',DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT IFNULL(creator_id,0)) as 'old_user' " \
                      "from analysis_period WHERE DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}' AND is_new = 0 " \
                      "GROUP BY posterid,DATE_FORMAT(start_at,'%Y-%m-%d') ORDER BY posterid, DATE_FORMAT(start_at,'%Y-%m-%d');".format(
                date=date)
        # df_old_user = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date'])
        df_old_user = pd.read_sql(str_sql, self.__mysql_conn)
        # 地域分布
        if date is None:
            str_sql = " select a.posterid as 'poster_id', DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',b.province,COUNT(DISTINCT IFNULL(b.province,0)) as 'num'" \
                      " from analysis_period a,alatting_website_ipaddress b where a.ip_id = b.id GROUP BY a.posterid,DATE_FORMAT(start_at,'%Y-%m-%d'),b.province ORDER BY a.posterid,DATE_FORMAT(start_at,'%Y-%m-%d');"
        else:
            str_sql = " select a.posterid as 'poster_id', DATE_FORMAT(start_at,'%Y-%m-%d') as 'date',b.province,COUNT(DISTINCT IFNULL(b.province,0)) as 'num'" \
                      " from analysis_period a,alatting_website_ipaddress b where a.ip_id = b.id AND  DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}'" \
                      " GROUP BY a.posterid,DATE_FORMAT(start_at,'%Y-%m-%d'),b.province;".format(date=date)
        df_addr = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'province'])
        addr_ret_list = []
        for poster_id in set(df_addr.index.get_level_values(0)):
            for date_index in set(df_addr.ix[poster_id].index.get_level_values(0)):
                addr_ret_dict = {}  # 海报每日地域分布dict
                addr_dict = {}  # 地域分布dict
                df_poster = df_addr.ix[poster_id, date_index]
                for addr in address_list:
                    try:
                        num = df_poster.ix[addr].num
                        addr_dict[addr] = int(num)
                    except KeyError:
                        addr_dict[addr] = 0
                addr_ret_dict["poster_id"] = poster_id
                addr_ret_dict["date"] = str(date_index)
                addr_ret_dict["addr"] = json.dumps(addr_dict)
                addr_ret_list.append(addr_ret_dict)
        str_json_addr = json.dumps(addr_ret_list)
        df_addr2 = pd.read_json(str_json_addr, orient='records', dtype=False, convert_dates=False)
        df = pd.merge(left=df_vv, right=df_ip, how='left', on=['poster_id', 'date'])
        df = pd.merge(left=df, right=df_pv, how='left', on=['poster_id', 'date'])
        df = pd.merge(left=df, right=df_uv, how='left', on=['poster_id', 'date'])
        df = pd.merge(left=df, right=df_new_user, how='left', on=['poster_id', 'date'])
        df = pd.merge(left=df, right=df_old_user, how='left', on=['poster_id', 'date'])
        if df_addr2.empty is not True:
            df = pd.merge(left=df, right=df_addr2, how='left', on=['poster_id', 'date'])
        else:
            df["addr"] = [""]
        df = df.fillna(0)
        pd.set_option('precision', 0)
        page_json = df.to_json(orient='records')
        page_obj_list = json.loads(page_json)
        for kwargs in page_obj_list:
            num = self.session.query(PageView).filter_by(poster_id=kwargs["poster_id"], date=kwargs["date"]).update(
                {PageView.addr: kwargs["addr"],
                 PageView.uv: kwargs["uv"],
                 PageView.ip: kwargs["ip"],
                 PageView.pv: kwargs["pv"],
                 PageView.vv: kwargs["vv"],
                 PageView.new_user: kwargs["new_user"],
                 PageView.old_user: kwargs["old_user"]
                 })
            if num == 0:
                poster_view = PageView(**kwargs)
                self.session.add(poster_view)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 统计浏览器分布数据因子
    def calc_browser_view(self, date=None):
        if date is None:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id', " \
                      " CASE b.browser WHEN 'Mobile Safari' THEN 'safari' WHEN 'Chrome' THEN 'chrome' ELSE 'other' END  as browsertype,COUNT(browser) as 'num' " \
                      " from analysis_period as a, analysis_visitor as b where a.creator_id = b.id group by a.posterid,DATE_FORMAT(a.start_at,'%Y-%m-%d'),browsertype;"
        else:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id', " \
                      " CASE b.browser WHEN 'Mobile Safari' THEN 'safari' WHEN 'Chrome' THEN 'chrome' ELSE 'other' END  as browsertype,COUNT(browser) as 'num' " \
                      " from analysis_period as a, analysis_visitor as b where a.creator_id = b.id and DATE_FORMAT(a.start_at,'%Y-%m-%d') = '{date}' " \
                      " group by a.posterid,DATE_FORMAT(a.start_at,'%Y-%m-%d'),browsertype;".format(date=date)
        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'browsertype'])
        data_list = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[str(poster_id)].index.get_level_values(0)):
                browser_dict = {"poster_id": poster_id, "date": date}
                browserdf = df.ix[poster_id, date]
                for browser in ['ie', 'chrome', 'safari', 'firefox', 'opera', 'other']:
                    try:
                        num = browserdf.ix[browser].num
                        browser_dict[browser] = num
                    except KeyError:
                        browser_dict[browser] = 0
                data_list.append(browser_dict)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(BrowserView).filter_by(poster_id=kwargs["poster_id"],
                                                            date=kwargs["date"]).update({
                            BrowserView.ie: kwargs["ie"],
                            BrowserView.chrome: kwargs["chrome"],
                            BrowserView.safari: kwargs["safari"],
                            BrowserView.firefox: kwargs["firefox"],
                            BrowserView.opera: kwargs["opera"],
                            BrowserView.other: kwargs["other"]
                        })
            if num == 0:
                browser_view = BrowserView(**kwargs)
                self.session.add(browser_view)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 统计操作系统信息
    def calc_system_view(self, date=None):
        if date is None:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id'," \
                      " CASE b.system WHEN 'iOS' THEN 'ios' WHEN 'Android' THEN 'android' ELSE 'other' END as 'system',COUNT(system) as 'num' " \
                      " from analysis_period as a, analysis_visitor as b where a.creator_id = b.id GROUP BY a.posterid,DATE_FORMAT(a.start_at,'%Y-%m-%d'),b.system" \
                      " ORDER BY poster_id,date;"
        else:
            str_sql = " select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id'," \
                      " CASE b.system WHEN 'iOS' THEN 'ios' WHEN 'Android' THEN 'android' ELSE 'other' END as 'system',COUNT(system) as 'num' " \
                      " from analysis_period as a, analysis_visitor as b where a.creator_id = b.id AND DATE_FORMAT(a.start_at,'%Y-%m-%d') = '{date}' " \
                      " GROUP BY a.posterid,DATE_FORMAT(a.start_at,'%Y-%m-%d'),b.system" \
                      " ORDER BY poster_id,date;".format(date=date)
        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'system'])
        data_list = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[str(poster_id)].index.get_level_values(0)):
                system_dict = {"poster_id": poster_id, "date": date}
                dfItem = df.ix[poster_id].ix[date]
                for system in ['windows', 'ios', 'android', 'other']:
                    try:
                        num = dfItem.ix[system].num
                        system_dict[system] = num
                    except KeyError:
                        system_dict[system] = 0
                data_list.append(system_dict)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(SystemView).filter_by(poster_id=kwargs["poster_id"],
                                                           date=kwargs["date"]).update({
                SystemView.windows: kwargs["windows"],
                SystemView.ios: kwargs["ios"],
                SystemView.android: kwargs["android"],
                SystemView.other: kwargs["other"]
            })
            if num == 0:
                systemObj = SystemView(**kwargs)
                self.session.add(systemObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 屏幕信息统计
    def calc_screen_view(self, date=None):
        if date is None:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id'," \
                      "CASE  WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 768 THEN 'sml' " \
                      "WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) >= 768 AND CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 960  THEN 'mid' " \
                      "ELSE 'big' END AS 'screen',COUNT(*) as 'num' from analysis_period as a, analysis_visitor as b " \
                      "where a.creator_id = b.id GROUP BY date,posterid,screen;"
        else:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id'," \
                      "CASE  WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 768 THEN 'sml' " \
                      "WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) >= 768 AND CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 960  THEN 'mid' " \
                      "ELSE 'big' END AS 'screen',COUNT(*) as 'num' from analysis_period as a, analysis_visitor as b " \
                      "where a.creator_id = b.id AND DATE_FORMAT(a.start_at,'%Y-%m-%d') = '{date}' GROUP BY date,posterid,screen;".format(
                date=date)

        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'screen'])
        data_list = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[str(poster_id)].index.get_level_values(0)):
                screen_dict = {"poster_id": poster_id, "date": date}
                screen_df = df.ix[poster_id].ix[date]
                for screen in ['big', 'mid', 'sml']:
                    try:
                        num = screen_df[screen].num
                        screen_dict[screen] = int(num)
                    except KeyError:
                        screen_dict[screen] = 0
                data_list.append(screen_dict)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(ScreenView).filter_by(poster_id=kwargs["poster_id"],
                                                           date=kwargs["date"]).update({
                ScreenView.big: kwargs["big"],
                ScreenView.sml: kwargs["sml"],
                ScreenView.mid: kwargs["mid"]
            })
            if num == 0:
                screenObj = ScreenView(**kwargs)
                self.session.add(screenObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 设备信息统计
    def calc_device_view(self, date=None):
        if date is None:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id'," \
                      "CASE  WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 768 THEN 'phone' " \
                      "WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) >= 768 AND CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 960  THEN 'pad' " \
                      "ELSE 'pc' END AS 'device',COUNT(*) as 'num' from analysis_period as a, analysis_visitor as b " \
                      "where a.creator_id = b.id GROUP BY date,posterid,device;"
        else:
            str_sql = "select DATE_FORMAT(a.start_at,'%Y-%m-%d') as 'date', a.posterid as 'poster_id'," \
                      "CASE  WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 768 THEN 'phone' " \
                      "WHEN  CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) >= 768 AND CAST(SUBSTRING_INDEX(b.screen_size,'*',1) AS UNSIGNED ) < 960  THEN 'pad' " \
                      "ELSE 'pc' END AS 'device',COUNT(*) as 'num' from analysis_period as a, analysis_visitor as b " \
                      "where a.creator_id = b.id AND DATE_FORMAT(a.start_at,'%Y-%m-%d') = '{date}' GROUP BY date,posterid,device;".format(
                date=date)
        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'device'])
        data_list = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[str(poster_id)].index.get_level_values(0)):
                screen_dict = {"poster_id": poster_id, "date": date}
                screen_df = df.ix[poster_id].ix[date]
                for screen in ['pc', 'pad', 'phone']:
                    try:
                        num = screen_df[screen].num
                        screen_dict[screen] = num
                    except KeyError:
                        screen_dict[screen] = 0
                data_list.append(screen_dict)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(DeviceView).filter_by(poster_id=kwargs["poster_id"],
                                                           date=kwargs["date"]).update({
                DeviceView.phone: kwargs["phone"],
                DeviceView.pad: kwargs["pad"],
                DeviceView.pc: kwargs["pc"]
            })
            if num == 0:
                screenObj = DeviceView(**kwargs)
                self.session.add(screenObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 来源信息统计
    def calc_source_view(self, date=None):
        str_sql = " SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date',b.posterid as 'poster_id', CASE WHEN a.from_host LIKE '%baidu%' THEN 'baidu'" \
                  " WHEN a.from_host LIKE '%soso%' THEN 'soso' WHEN a.from_host LIKE '%google%' THEN 'google' WHEN a.from_host LIKE '%biying%' THEN 'biying'" \
                  " WHEN a.from_host LIKE '%sogou%' THEN 'sogou' WHEN a.from_host LIKE '%wechat%' THEN 'weixin' WHEN a.from_host LIKE '%weibo%' THEN 'weibo' " \
                  " WHEN a.from_host LIKE '%qzone%' THEN 'qzone' WHEN a.from_host LIKE '%facebook%' THEN 'facebook' " \
                  " WHEN a.from_host LIKE 'twitter' THEN 'twitter' ELSE 'other' END as 'source',COUNT(*) as 'num'"
        if date is None:
            str_sql += " FROM analysis_logs a,analysis_period b where a.period_id = b.id GROUP BY source,posterid,date;"
        else:
            str_sql += " FROM analysis_logs a,analysis_period b where a.period_id = b.id AND DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}' GROUP BY source,posterid,date;".format(
                date=date)
        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'source'])
        data_list = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[str(poster_id)].index.get_level_values(0)):
                source_dict = {"poster_id": poster_id, "date": date}
                dfItem = df.ix[poster_id, date]
                for system in ['baidu', 'soso', 'google', 'biying', 'sogou', 'weixin', 'weibo', 'qzone', 'facebook',
                               'twitter', 'other']:
                    try:
                        num = dfItem.ix[system].num
                        source_dict[system] = num
                    except KeyError:
                        source_dict[system] = 0
                data_list.append(source_dict)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(SourceView).filter_by(poster_id=kwargs["poster_id"],
                                                           date=kwargs["date"]).update({
                SourceView.baidu: kwargs["baidu"],
                SourceView.soso: kwargs["soso"],
                SourceView.google: kwargs["google"],
                SourceView.biying: kwargs["biying"],
                SourceView.sogou: kwargs["sogou"],
                SourceView.weibo: kwargs["weibo"],
                SourceView.qzone: kwargs["qzone"],
                SourceView.facebook: kwargs["facebook"],
                SourceView.twitter: kwargs["twitter"],
                SourceView.other: kwargs["other"]
            })
            if num == 0:
                sourceViewObj = SourceView(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 页面停留时长
    def calc_time_dist_view(self, date=None):
        # 查询页面停留时间
        if date is None:
            str_sql = "SELECT posterid AS 'poster_id', CASE WHEN during_time < 60 THEN 'l_xs' WHEN during_time >= 60 and during_time < 240 THEN 'l_sm'" \
                      "WHEN during_time >= 240 and during_time <300 THEN 'l_md' ELSE  'l_lg' END as 'stay',DATE_FORMAT(start_at,'%Y-%m-%d') as 'date'," \
                      "COUNT(*) as 'num' from analysis_period GROUP BY posterid,stay,DATE_FORMAT(start_at,'%Y-%m-%d') ORDER BY posterid, date;"
        else:
            str_sql = "SELECT posterid AS 'poster_id', CASE WHEN during_time < 60 THEN 'l_xs' WHEN during_time >= 60 and during_time < 240 THEN 'l_sm'" \
                      "WHEN during_time >= 240 and during_time <300 THEN 'l_md' ELSE  'l_lg' END as 'stay',DATE_FORMAT(start_at,'%Y-%m-%d') as 'date'," \
                      "COUNT(*) as 'num' from analysis_period WHERE DATE_FORMAT(start_at,'%Y-%m-%d') = '{date}' GROUP BY posterid,stay,DATE_FORMAT(start_at,'%Y-%m-%d') " \
                      "ORDER BY posterid, date;".format(date=date)

        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'stay'])
        data_list = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[str(poster_id)].index.get_level_values(0)):
                page_detail_dict = {"poster_id": poster_id, "date": date}
                dfItem = df.ix[poster_id, date]
                view_num = 0
                for stay in ['l_xs', 'l_sm', 'l_md', 'l_lg']:
                    try:
                        num = dfItem.ix[stay].num
                        view_num += num
                        page_detail_dict[stay] = num
                    except KeyError:
                        page_detail_dict[stay] = 0
                        view_num += 0
                page_detail_dict["v_num"] = view_num
                data_list.append(page_detail_dict)

        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(PagesDetailView).filter_by(poster_id=kwargs["poster_id"],
                                                                date=kwargs["date"]).update({
                PagesDetailView.l_xs: kwargs["l_xs"],
                PagesDetailView.l_sm: kwargs["l_sm"],
                PagesDetailView.l_md: kwargs["l_md"],
                PagesDetailView.l_lg: kwargs["l_lg"],
                PagesDetailView.v_num: kwargs["v_num"]
            })
            if num == 0:
                sourceViewObj = PagesDetailView(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 产品信息统计
    def calc_poster_product_view(self, date=None):
        # 统计产品view
        if date is None:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', a.product_id ,b.poster_id,COUNT(a.product_id) as 'view'" \
                      " from analysis_productviewlogs as a, product_product as b WHERE a.product_id = b.id GROUP BY date,poster_id,product_id;"
        else:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', a.product_id ,b.poster_id,COUNT(a.product_id) as 'view'" \
                      " from analysis_productviewlogs as a, product_product as b WHERE a.product_id = b.id and DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY date,poster_id,product_id;".format(date=date)
        # df_view1 = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'product_id', 'date'])
        df_view = pd.read_sql(str_sql, self.__mysql_conn)
        # 统计产品加入购物车的数目
        if date is None:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', a.product_id ,b.poster_id,COUNT(a.product_id) as 'cart' " \
                      "from product_cart as a, product_product as b where a.product_id = b.id GROUP BY date,poster_id,product_id;"
        else:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', a.product_id ,b.poster_id,COUNT(a.product_id) as 'cart' " \
                      "from product_cart as a, product_product as b where a.product_id = b.id AND DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY date,poster_id,product_id;".format(date=date)
        # df_cart1 = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'product_id', 'date'])
        df_cart = pd.read_sql(str_sql, self.__mysql_conn)
        # 统计产品订单数目
        if date is None:
            str_sql = "select CAST(b.post_id AS UNSIGNED) as 'poster_id',DATE_FORMAT(b.created_at,'%Y-%m-%d') as 'date',a.productid as 'product_id'," \
                      "COUNT(a.productid) as 'order' from product_productsnapshot as a, product_order as b where a.order_id = b.id GROUP BY b.post_id,date,productid;"
        else:
            str_sql = "select CAST(b.post_id AS UNSIGNED) as 'poster_id',DATE_FORMAT(b.created_at,'%Y-%m-%d') as 'date',a.productid as 'product_id'," \
                      "COUNT(a.productid) as 'order' from product_productsnapshot as a, product_order as b where a.order_id = b.id AND DATE_FORMAT(b.created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY b.post_id,date,productid;".format(date=date)
        df_order = pd.read_sql(str_sql, self.__mysql_conn)
        if df_view.empty is True:
            return
        df = pd.merge(left=df_view, right=df_cart, how='left', on=['poster_id', 'product_id', 'date'])
        df = pd.merge(left=df, right=df_order, how='left', on=['poster_id', 'product_id', 'date'])
        df = df.fillna(0)
        str_json = df.to_json(orient='records')
        obj_list = json.loads(str_json)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in obj_list:
            num = self.session.query(Product).filter_by(poster_id=kwargs["poster_id"], product_id=kwargs["product_id"],
                                                        date=kwargs["date"]).update({
                Product.view: kwargs["view"],
                Product.cart: kwargs["cart"],
                Product.order: kwargs["order"]
            })
            if num == 0:
                sourceViewObj = Product(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 优惠券统计信息
    def calc_coupon_view(self, date=None):
        # 统计产品view
        # coupon view
        if date is None:
            str_sql = "select DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.coupon_id,COUNT(a.coupon_id) as 'view' " \
                      "from analysis_couponviewlogs as a,coupon_coupon as b where a.coupon_id = b.id GROUP BY poster_id,a.coupon_id,date;"
        else:
            str_sql = "select DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.coupon_id,COUNT(a.coupon_id) as 'view' " \
                      "from analysis_couponviewlogs as a,coupon_coupon as b where a.coupon_id = b.id AND  DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}'" \
                      " GROUP BY poster_id,a.coupon_id,date;".format(date=date)
        df_view = pd.read_sql(str_sql, self.__mysql_conn)
        # coupon fetch
        if date is None:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.coupon_id,COUNT(a.coupon_id) as 'fetch' " \
                      "FROM coupon_codes as a, coupon_coupon as b where a.coupon_id = b.id GROUP BY poster_id,a.coupon_id,date;"
        else:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.coupon_id,COUNT(a.coupon_id) as 'fetch' " \
                      "FROM coupon_codes as a, coupon_coupon as b where a.coupon_id = b.id AND DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}'" \
                      " GROUP BY poster_id,a.coupon_id,date;".format(date=date)
        df_fetch = pd.read_sql(str_sql, self.__mysql_conn)
        # coupon use
        if date is None:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.coupon_id,COUNT(a.coupon_id) as 'use' " \
                      "FROM coupon_codes as a, coupon_coupon as b where a.is_used = 1 and a.coupon_id = b.id GROUP BY poster_id,a.coupon_id,date;"
        else:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.coupon_id,COUNT(a.coupon_id) as 'use' " \
                      "from coupon_codes as a, coupon_coupon as b where DATE_FORMAT(a.created_at,'%Y-%m-%d')= '{date}' " \
                      "AND a.is_used = 1 and a.coupon_id = b.id GROUP BY poster_id,a.coupon_id,date;".format(date=date)
        df_use = pd.read_sql(str_sql, self.__mysql_conn)
        if df_view.empty is True:
            return
        df = pd.merge(left=df_view, right=df_fetch, how='left', on=['poster_id', 'coupon_id', 'date'])
        df = pd.merge(left=df, right=df_use, how='outer', on=['poster_id', 'coupon_id', 'date'])
        df = df.fillna(0)
        str_json = df.to_json(orient='records')
        couponObjList = json.loads(str_json)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in couponObjList:
            num = self.session.query(Coupon).filter_by(poster_id=kwargs["poster_id"], coupon_id=kwargs["coupon_id"],
                                                       date=kwargs["date"]).update({
                Coupon.view: kwargs["view"],
                Coupon.fetch: kwargs["fetch"],
                Coupon.use: kwargs["use"]
            })
            if num == 0:
                sourceViewObj = Coupon(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 名片统计信息表
    def calc_card_view(self, date=None):
        # card view
        if date is None:
            str_sql = "select DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.card_id,COUNT(a.card_id) as 'view' " \
                      "from analysis_cardviewlogs as a,card_cards as b where a.card_id = b.id GROUP BY poster_id,a.card_id,date;"
        else:
            str_sql = "select DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date', b.poster_id,a.card_id,COUNT(a.card_id) as 'view' " \
                      "from analysis_cardviewlogs as a,card_cards as b where a.card_id = b.id AND DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}'" \
                      " GROUP BY poster_id,a.card_id,date;".format(date=date)
        df_vew = pd.read_sql(str_sql, self.__mysql_conn)
        # card favorite
        if date is None:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date',b.poster_id,a.cards_id as 'card_id',COUNT(a.cards_id) as 'favorite' " \
                      "from card_cardbookmark as a,card_cards as b where a.cards_id = b.id GROUP BY poster_id,a.cards_id,date;"
        else:
            str_sql = "SELECT DATE_FORMAT(a.created_at,'%Y-%m-%d') as 'date',b.poster_id,a.cards_id as 'card_id',COUNT(a.cards_id) as 'favorite' " \
                      "from card_cardbookmark as a,card_cards as b where a.cards_id = b.id AND  DATE_FORMAT(a.created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY poster_id,a.cards_id,date;".format(date=date)
        df_favorite = pd.read_sql(str_sql, self.__mysql_conn)
        if df_vew.empty == True:
            return
        df = pd.merge(left=df_vew, right=df_favorite, how='left', on=['poster_id', 'card_id', 'date'])
        df = df.fillna(0)
        pd.set_option("precision", 2)
        str_json = df.to_json(orient='records')
        obj_list = json.loads(str_json)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in obj_list:
            num = self.session.query(Card).filter_by(poster_id=kwargs["poster_id"], card_id=kwargs["card_id"],
                                                     date=kwargs["date"]).update({
                Card.view: kwargs["view"],
                Card.favorite: kwargs["favorite"]
            })
            if num == 0:
                sourceViewObj = Coupon(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 统计常规交互行为
    def calc_normal_action_view(self, date=None):
        # 评论数
        if date is None:
            str_sql = "SELECT poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'comment' " \
                      "from alatting_website_comment GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id"
        else:
            str_sql = "SELECT poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'comment' " \
                      "from alatting_website_comment WHERE DATE_FORMAT(created_at,'%Y-%m-%d')  = '{date}' " \
                      "GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id".format(date=date)
        df_comment = pd.read_sql(str_sql, self.__mysql_conn)
        # 收藏数
        if date is None:
            str_sql = "select poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'favorite' " \
                      "from alatting_website_posterfavorites GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id;"
        else:
            str_sql = "select poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'favorite' " \
                      "from alatting_website_posterfavorites WHERE DATE_FORMAT(created_at,'%Y-%m-%d')  = '{date}' " \
                      "GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id;".format(date=date)
        df_favorite = pd.read_sql(str_sql, self.__mysql_conn)
        # 点赞数
        if date is None:
            str_sql = "SELECT poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'prize' " \
                      "FROM alatting_website_posterfun GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id;"
        else:
            str_sql = "SELECT poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'prize' " \
                      "FROM alatting_website_posterfun WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id;".format(date=date)
        df_prize = pd.read_sql(str_sql, self.__mysql_conn)
        # 评分数
        if date is None:
            str_sql = "select poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'rate' " \
                      "from alatting_website_rating GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id"
        else:
            str_sql = "select poster_id,DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'rate' " \
                      "from alatting_website_rating WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),poster_id".format(date=date)
        df_rate = pd.read_sql(str_sql, self.__mysql_conn)
        df = df_comment
        if df_comment.empty != True or df_favorite.empty !=True:
            df = pd.merge(left=df_comment, right=df_favorite, how='outer', on=['poster_id', 'date'])
        if df.empty != True or df_prize.empty != True:
            df = pd.merge(left=df, right=df_prize, how='outer', on=['poster_id', 'date'])
        if df.empty != True or df_rate.empty != True:
            df = pd.merge(left=df, right=df_rate, how='outer', on=['poster_id', 'date'])
        df = df.fillna(0)
        pd.set_option("precision", 2)
        str_json = df.to_json(orient='records')
        obj_list = json.loads(str_json)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in obj_list:
            num = self.session.query(NormalAction).filter_by(poster_id=kwargs["poster_id"],
                                                             date=kwargs["date"]).update({
                NormalAction.rate: kwargs.get("rate",0),
                NormalAction.favorite: kwargs.get("favorite",0),
                NormalAction.comment: kwargs.get("comment",0),
                NormalAction.prize: kwargs.get("prize",0),
            })
            if num == 0:
                sourceViewObj = NormalAction(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                continue

    # 海报事件统计
    def calc_event_action_view(self, date=None):
        if date is None:
            str_sql = "select DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',poster_id,name,source,stype as 'type',COUNT(source) as 'nums' " \
                      "from analysis_sourcelog GROUP BY DATE(created_at),poster_id,source,name;"
        else:
            str_sql = "select DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',poster_id,name,source,stype as 'type',COUNT(source) as 'nums' " \
                      "from analysis_sourcelog WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}' " \
                      "GROUP BY DATE(created_at),poster_id,source,name;".format(date=date)
        df = pd.read_sql(str_sql, self.__mysql_conn)
        ret_json = df.to_json(orient="records")
        eventObjList = json.loads(ret_json)

        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in eventObjList:
            num = self.session.query(EventAction).filter_by(poster_id=kwargs["poster_id"],name=kwargs["name"],source=kwargs["source"],type=kwargs["type"],
                                                            date=kwargs["date"]).update({
                EventAction.nums: kwargs["nums"]
            })
            if num == 0:
                sourceViewObj = EventAction(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                continue


    # 海报分享信息统计
    def calc_share_action_view(self, date=None):
        if date is None:
            str_sql = "SELECT posterid as 'poster_id',CASE WHEN to_s LIKE '%qzone%' OR to_s LIKE '%qq%' THEN 'qzone' WHEN to_s LIKE '%weibo%' THEN 'weibo' " \
                      " WHEN to_s LIKE '%wechat%' OR to_s LIKE '%wx%' THEN 'weixin' WHEN to_s LIKE '%facebook%' THEN 'facebook' " \
                      " WHEN to_s LIKE '%twitter%' THEN 'twitter' ELSE 'other' END as 'channel',DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'num' " \
                      " from analysis_sharelist GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),posterid,channel "
        else:
            str_sql = "SELECT posterid as 'poster_id',CASE WHEN to_s LIKE '%qzone%' OR to_s LIKE '%qq%' THEN 'qzone' WHEN to_s LIKE '%weibo%' THEN 'weibo' " \
                      " WHEN to_s LIKE '%wechat%' OR to_s LIKE '%wx%' THEN 'weixin' WHEN to_s LIKE '%facebook%' THEN 'facebook' " \
                      " WHEN to_s LIKE '%twitter%' THEN 'twitter' ELSE 'other' END as 'channel',DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(*) as 'num' " \
                      " from analysis_sharelist WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}' GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d'),posterid,channel ".format(
                date=date)
        df = pd.read_sql(str_sql, self.__mysql_conn, index_col=['poster_id', 'date', 'channel'])
        data_list  = []
        for poster_id in set(df.index.get_level_values(0)):
            for date in set(df.ix[poster_id].index.get_level_values(0)):
                dfItem = df.ix[poster_id].ix[date]
                share_dict = {"poster_id":poster_id, "date":date}
                for channel in ['weixin', 'weibo', 'qzone', 'facebook', 'twitter', 'other']:
                    try:
                        num = dfItem.ix[channel].num
                        share_dict[channel] = num
                    except KeyError:
                        share_dict[channel] = 0
                data_list.append(share_dict)
        # 存数据库，如果存在则更新，否则做插入操作
        for kwargs in data_list:
            num = self.session.query(ShareAction).filter_by(poster_id=kwargs["poster_id"],
                                                            date=kwargs["date"]).update({
                ShareAction.weixin: kwargs["weixin"],
                ShareAction.weibo: kwargs["weibo"],
                ShareAction.qzone: kwargs["qzone"],
                ShareAction.facebook: kwargs["facebook"],
                ShareAction.twitter: kwargs["twitter"],
                ShareAction.other: kwargs["other"]
            })
            if num == 0:
                sourceViewObj = ShareAction(**kwargs)
                self.session.add(sourceViewObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()


    # 站点统计概况
    def calc_website_summary(self):
        websiteSumaryObj = WebsiteSummary(id=1)
        # 皮肤总数skin
        str_sql = "SELECT COUNT(id) as 'skin' from poster_systemskin;"
        df_skin = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.skin = df_skin.skin.values[0]
        # template
        str_sql = "SELECT COUNT(id) as 'template' FROM alatting_website_template;"
        df_temp = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.template = df_temp.template.values[0]
        # poster
        str_sql = "SELECT COUNT(id) as 'poster' FROM alatting_website_poster;"
        df_poster = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.poster = df_poster.poster.values[0]
        # approve_poster
        str_sql = "SELECT COUNT(id) as 'approve_poster' FROM alatting_website_poster WHERE `status` in ('Checked','Published');"
        df_approve_poster = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.approve_poster = df_approve_poster.approve_poster.values[0]
        # pv
        str_sql = "SELECT COUNT(id) as 'pv' FROM account_visitorfrequency;"
        df_pv = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.pv = df_pv.pv.values[0]
        # uv
        str_sql = "SELECT COUNT(DISTINCT visitor_info) as 'uv' FROM account_visitorfrequency;"
        df_uv = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.uv = df_uv.uv.values[0]
        # ip
        str_sql = "SELECT COUNT(DISTINCT visitor_ip) as 'ip' FROM account_visitorfrequency;"
        df_ip = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.ip = df_ip.ip.values[0]
        # vv
        str_sql = "SELECT COUNT(id) as 'vv' from account_visitorfrequency;"
        df_vv = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.vv = df_vv.vv.values[0]
        # user
        str_sql = "SELECT COUNT(id) as 'user' from auth_user;"
        df_user = pd.read_sql(str_sql, self.__mysql_conn)
        websiteSumaryObj.user = df_user.user.values[0]
        websiteSumaryObj.update_at = tools.get_current_date('%Y-%m-%d %H:%M:%S')
        self.session.merge(websiteSumaryObj)
        self.session.commit()

    # 站点用戶资源统计
    def calc_website_user_summary(self, date=None):
        cur = tools.get_current_date(format='%Y-%m-%d %H:%M:%S')
        date_list = tools.gen_time_serials(start='2016-10-01 00:00:00', end=cur, freq='D', date_format='%Y-%m-%d')
        # pv
        if date is None:
            str_sql = "SELECT DATE_FORMAT(created_at,'%Y-%m-%d') as 'date', COUNT(id) as 'pv' FROM account_visitorfrequency GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d');"
            df_pv = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            df_pv = df_pv.reindex(date_list)
            # login
            str_sql = "SELECT DATE_FORMAT(created_at,'%Y-%m-%d') as 'date', COUNT(DISTINCT user_id) as 'login' " \
                      "from account_userloginfrequency GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d');"
            df_login = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            # register
            str_sql = "SELECT DATE_FORMAT(date_joined,'%Y-%m-%d') AS 'date',COUNT(id) AS 'register' " \
                      "from auth_user GROUP BY DATE_FORMAT(date_joined,'%Y-%m-%d') ORDER BY DATE(date_joined);"
            df_register = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            df_register = df_register.reindex(date_list).fillna(0)
            # 每一天的用户总数
            user_list = []
            tools.cal_pre_nums(src=df_register.register.values,tgt=user_list)
            df_register["user"] = user_list  # 每一天的用户总数
            # sign
            str_sql = "SELECT DATE_FORMAT(create_time,'%Y-%m-%d') as 'date',COUNT(DISTINCT user_id) as 'sign' FROM account_signinhistory GROUP BY DATE(create_time);"
            df_sign = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            df = df_pv.join([df_login, df_register, df_sign])
            df = df.fillna(0)
            df = df[~df.user.isin([0])]
            # live_rate
            df["live_rate"] = (df.login / df.user) * 100.0
            # register_rate
            df["register_rate"] = [0 if (df.register.ix[index] == 0 or df.pv.ix[index] == 0) else (df.register.ix[index] /
                                                                                                   df.pv.ix[index]) * 100.0
                                   for index in df.index]
            pd.set_option("precision", 2)
            df = df.reset_index()
            result_list = df.to_json(orient='records')
            data_list = json.loads(result_list)
            # 存数据库，如果存在则更新，否则做插入操作
            for kwargs in data_list:
                num = self.session.query(WebsiteUserSummary).filter_by(date=kwargs["date"]).update({
                    WebsiteUserSummary.user: kwargs["user"],
                    WebsiteUserSummary.register: kwargs["register"],
                    WebsiteUserSummary.login: kwargs["login"],
                    WebsiteUserSummary.pv: kwargs["pv"],
                    WebsiteUserSummary.sign: kwargs["sign"],
                    WebsiteUserSummary.register_rate: kwargs["register_rate"],
                    WebsiteUserSummary.live_rate: kwargs["live_rate"]
                })
                if num == 0:
                    sourceViewObj = WebsiteUserSummary(**kwargs)
                    self.session.add(sourceViewObj)
                try:
                    self.session.commit()
                except Exception as e:
                    self.session.rollback()
                    continue
        else:
            str_sql = "SELECT COUNT(id) as 'pv' FROM account_visitorfrequency WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}'".format(date=date)
            df_pv = pd.read_sql(str_sql, self.__mysql_conn)
            pv = df_pv.pv.values[0]
            # login
            str_sql = "SELECT COUNT(DISTINCT user_id) as 'login' from account_userloginfrequency WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}'".format(date=date)
            df_login = pd.read_sql(str_sql, self.__mysql_conn)
            login = df_login.login.values[0]
            # register
            str_sql = "SELECT COUNT(id) AS 'register' from auth_user WHERE DATE_FORMAT(date_joined,'%Y-%m-%d') = '{date}'".format(date=date)
            df_register = pd.read_sql(str_sql, self.__mysql_conn)
            register = df_register.register.values[0]
            #用戶总数
            str_sql = "SELECT COUNT(id) AS 'user' FROM auth_user "
            df = pd.read_sql(str_sql,self.__mysql_conn)
            user = df.user.values[0]

            # sign
            str_sql = "SELECT COUNT(DISTINCT user_id) as 'sign' FROM account_signinhistory WHERE DATE_FORMAT(create_time,'%Y-%m-%d') = '{date}'".format(date=date)
            df_sign = pd.read_sql(str_sql, self.__mysql_conn)
            sign = df_sign.sign.values[0]
            # live_rate
            if user != 0:
                live_rate = (sign / user) * 100.0
                register_rate = (register/user) * 100.0
            else:
                live_rate = 0
                register_rate = 0
            num = self.session.query(WebsiteUserSummary).filter_by(date=date).update({
                WebsiteUserSummary.user:user,
                WebsiteUserSummary.register:register,
                WebsiteUserSummary.login:login,
                WebsiteUserSummary.pv:pv,
                WebsiteUserSummary.sign:sign,
                WebsiteUserSummary.register_rate:register_rate,
                WebsiteUserSummary.live_rate:live_rate
            })
            if num == 0:
                sitUserObj = WebsiteUserSummary(date=date,user=user,register=register,register_rate=register_rate,login=login,live_rate=live_rate,sign=sign,pv=pv)
                self.session.add(sitUserObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()

    # 站点海报资源概况
    def calc_website_poster_summary(self, date=None):
        self.session.commit()
        if date is None:
            cur = tools.get_current_date(format='%Y-%m-%d %H:%M:%S')
            date_list = tools.gen_time_serials(start='2016-01-01 00:00:00', end=cur, freq='D', date_format='%Y-%m-%d')
            # 新增的海报
            str_sql = "SELECT DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT id) as 'new' from alatting_website_poster GROUP BY DATE(created_at);"
            df_new = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            df_new = df_new.reindex(date_list).fillna(0)
            poster_list = []
            tools.cal_pre_nums(src=df_new.new.values,tgt=poster_list)
            df_new["poster"] = poster_list  # 每一天的海报总数
            # published 已发布的海报
            str_sql = "SELECT DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT id) as 'published' " \
                      "from alatting_website_poster WHERE status = 'Published' GROUP BY DATE(created_at);"
            df_pub = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            # approved 审核通过的海报
            str_sql = "SELECT DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT id) as 'approved' " \
                      "from alatting_website_poster WHERE status IN ('Published','Checked') GROUP BY DATE(created_at);"
            df_app = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            # refused 审核未通过的海报
            str_sql = "SELECT DATE_FORMAT(created_at,'%Y-%m-%d') as 'date',COUNT(DISTINCT id) as 'refused' " \
                      "from alatting_website_poster WHERE status = 'Uncheck' GROUP BY DATE(created_at);"
            df_ref = pd.read_sql(str_sql, self.__mysql_conn, index_col=['date'])
            df = df_new.join([df_pub, df_app, df_ref])
            df = df.fillna(0)
            df = df[~df.poster.isin([0])]
            # live_rate
            df["publish_rate"] = (df.published / df.poster) * 100.0
            # register_rate
            df["approve_rate"] = (df.approved / df.poster) * 100.0
            pd.set_option("precision", 2)
            df = df.reset_index()
            result_list = df.to_json(orient='records')
            data_list = json.loads(result_list)
            for kwargs in data_list:
                num = self.session.query(WebsitePosterSummary).filter_by(date=date).update({
                         WebsitePosterSummary.poster: kwargs["poster"],
                         WebsitePosterSummary.refused: kwargs["refused"],
                         WebsitePosterSummary.approved: kwargs["approved"],
                         WebsitePosterSummary.approve_rate: kwargs["approve_rate"],
                         WebsitePosterSummary.published: kwargs["published"],
                         WebsitePosterSummary.publish_rate: kwargs["publish_rate"]
                    })
                if num == 0:
                    sitPosterObj = WebsitePosterSummary(**kwargs)
                    self.session.add(sitPosterObj)
                try:
                    self.session.commit()
                except Exception as e:
                    self.session.rollback()
        else:
            # 新增的海报
            str_sql = "SELECT COUNT(DISTINCT id) as 'new' from alatting_website_poster WHERE DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}';".format(
                date=date)
            df_new = pd.read_sql(str_sql, self.__mysql_conn)
            new = df_new.new.values[0]
            # 海报总数
            str_sql = "SELECT COUNT(DISTINCT id) as 'poster' from alatting_website_poster "
            df_new = pd.read_sql(str_sql, self.__mysql_conn)
            poster = df_new.poster.values[0]
            # published 已发布的海报
            str_sql = "SELECT COUNT(DISTINCT id) as 'published' from alatting_website_poster WHERE status = 'Published' " \
                      "AND DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}';".format(date=date)
            df_pub = pd.read_sql(str_sql, self.__mysql_conn)
            published = df_pub.published.values[0]
            # approved 审核通过的海报
            str_sql = "SELECT  COUNT(DISTINCT id) as 'approved' from alatting_website_poster WHERE status IN ('Published','Checked') " \
                      "AND DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}';".format(date=date)
            df_app = pd.read_sql(str_sql, self.__mysql_conn)
            approved = df_app.approved.values[0]
            # refused 审核未通过的海报
            str_sql = "SELECT COUNT(DISTINCT id) as 'refused' from alatting_website_poster WHERE status = 'Uncheck' " \
                      "AND DATE_FORMAT(created_at,'%Y-%m-%d') = '{date}';".format(date=date)
            df_ref = pd.read_sql(str_sql, self.__mysql_conn)
            refused = df_ref.refused.values[0]
            # publish_rate
            if poster == 0:
                publish_rate = 0
                approve_rate = 0
            else:
                publish_rate = (published / poster) * 100.0
                # register_rate
                approve_rate = (approved / poster) * 100.0
            num = self.session.query(WebsitePosterSummary).filter_by(date=date).update(
                {WebsitePosterSummary.poster: poster, WebsitePosterSummary.refused: refused,
                 WebsitePosterSummary.approved: approved, WebsitePosterSummary.approve_rate: approve_rate,
                 WebsitePosterSummary.published: published, WebsitePosterSummary.publish_rate: publish_rate})
            if num == 0:
                sitPosterObj = WebsitePosterSummary(date=date, poster=poster, new=new, approved=approved,
                                                    approve_rate=approve_rate, published=published,
                                                    publish_rate=publish_rate, refused=refused)
                self.session.add(sitPosterObj)
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                print(e)

    def init_calc_data(self, date=None):
        logging.info("1.function calc_poster_page_view begin execute ...")
        self.calc_poster_page_view(date)

        logging.info("2.function calc_browser_view begin execute ...")
        self.calc_browser_view(date)

        logging.info("3.function calc_system_view begin execute ...")
        self.calc_system_view(date)

        logging.info("4.function calc_screen_view begin execute ...")
        self.calc_screen_view(date)

        logging.info("5.function calc_device_view begin execute ...")
        self.calc_device_view(date)

        logging.info("6.function calc_source_view begin execute ...")
        self.calc_source_view(date)

        logging.info("7.function calc_time_dist_view begin execute ...")
        self.calc_time_dist_view(date)

        logging.info("8.function calc_poster_product_view begin execute ...")
        self.calc_poster_product_view(date)

        logging.info("9.function calc_coupon_view begin execute ...")
        self.calc_coupon_view(date)

        logging.info("10.function calc_card_view begin execute ...")
        self.calc_card_view(date)

        logging.info("11.function calc_normal_action_view begin execute ...")
        self.calc_normal_action_view(date)

        logging.info("12.function calc_event_action_view begin execute ...")
        self.calc_event_action_view(date)

        logging.info("13.function calc_share_action_view begin execute ...")
        self.calc_share_action_view(date)

        logging.info("14.function calc_website_summary begin execute ...")
        self.calc_website_summary()

        logging.info("15.function calc_website_user_summary begin execute ...")
        self.calc_website_user_summary(date)

        logging.info("16.function calc_website_poster_summary begin execute ...")
        self.calc_website_poster_summary(date)


if __name__ == '__main__':
    mysqlClient = MysqlClient()
    time1 = time.time()
    cur = tools.get_current_date(format="%Y-%m-%d")
    processor = DataProcessor(mysql_client=mysqlClient.mysql_client)
    # processor.calc_poster_page_view(None)
    # processor.calc_browser_view(cur)
    # processor.calc_system_view(cur)
    # processor.calc_poster_product_view(cur)
    # processor.calc_card_view(cur)
    # processor.calc_coupon_view(cur)
    # processor.calc_normal_action_view(cur)
    # processor.calc_share_action_view(cur)
    # processor.calc_website_summary()
    # processor.calc_website_user_summary(cur)
    # processor.calc_event_action_view(cur)
    # processor.calc_source_view(cur)
    # processor.calc_time_dist_view(cur)
    processor.calc_website_user_summary(date=cur)
    # processor.calc_website_poster_summary()
    # processor.init_calc_data()
    # processor.calc_poster_page_view()
    time2 = time.time()
    print(time2 - time1)
    processor.session.close()
