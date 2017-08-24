from sqlalchemy import Column, Integer, String, Date, Float, TIMESTAMP
from . import BaseModel


# 海报浏览数据统计表
class PageView(BaseModel):
    __tablename__ = 'analysis_page_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    pv = Column(Integer, default=0)
    uv = Column(Integer, default=0)
    vv = Column(Integer, default=0)
    ip = Column(Integer, default=0)
    new_user = Column(Integer, default=0)
    old_user = Column(Integer, default=0)
    addr = Column(String(1024), default="")

    def __repr__(self):
        return "<PageView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 浏览器数据统计表
class BrowserView(BaseModel):
    __tablename__ = 'analysis_brower_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    ie = Column(Integer, default=0)
    chrome = Column(Integer, default=0)
    safari = Column(Integer, default=0)
    firefox = Column(Integer, default=0)
    opera = Column(Integer, default=0)
    other = Column(Integer, default=0)

    def __repr__(self):
        return "<BrowerView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 操作系统信息统计表
class SystemView(BaseModel):
    __tablename__ = 'analysis_system_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    windows = Column(Integer, default=0)
    ios = Column(Integer, default=0)
    android = Column(Integer, default=0)
    other = Column(Integer, default=0)

    def __repr__(self):
        return "<SystemView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 屏幕信息统计表
class ScreenView(BaseModel):
    __tablename__ = 'analysis_screen_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    big = Column(Integer, nullable=False, unique=True)
    mid = Column(Integer, default=0)
    sml = Column(Integer, default=0)

    def __repr__(self):
        return "<ScreenView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 设备信息统计表
class DeviceView(BaseModel):
    __tablename__ = 'analysis_os_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    pc = Column(Integer, nullable=False, unique=True)
    pad = Column(Integer, default=0)
    phone = Column(Integer, default=0)

    def __repr__(self):
        return "<DeviceView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 來源信息统计表
class SourceView(BaseModel):
    __tablename__ = 'analysis_from_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    baidu = Column(Integer, default=0)
    soso = Column(Integer, default=0)
    google = Column(Integer, default=0)
    biying = Column(Integer, default=0)
    sogou = Column(Integer, default=0)
    weixin = Column(Integer, default=0)
    weibo = Column(Integer, default=0)
    qzone = Column(Integer, default=0)
    facebook = Column(Integer, default=0)
    twitter = Column(Integer, default=0)
    other = Column(Integer, default=0)

    def __repr__(self):
        return "<DeviceView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 页面访问详情统计信息
class PagesDetailView(BaseModel):
    __tablename__ = 'analysis_page_detail_view'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    v_num = Column(Integer, default=0)
    l_xs = Column(Integer, default=0)
    l_sm = Column(Integer, default=0)
    l_md = Column(Integer, default=0)
    l_lg = Column(Integer, default=0)

    def __repr__(self):
        return "<PageView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 页面交互详情统计信息
class NormalAction(BaseModel):
    __tablename__ = 'analysis_normal_action'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    poster_id = Column(Integer, nullable=False, unique=True)
    comment = Column(Integer, nullable=False)
    favorite = Column(Integer, default=0)
    prize = Column(Integer, default=0)
    rate = Column(Integer, default=0)

    def __repr__(self):
        return "<NormalAction:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 海报分享统计信息
class ShareAction(BaseModel):
    __tablename__ = 'analysis_share_action'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    weixin = Column(Integer, nullable=False, unique=True)
    weibo = Column(Integer, default=0)
    qzone = Column(Integer, default=0)
    facebook = Column(Integer, default=0)
    twitter = Column(Integer, default=0)
    other = Column(Integer, default=0)

    def __repr__(self):
        return "<PageView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 海报事件统计信息
class EventAction(BaseModel):
    __tablename__ = 'analysis_event_action'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    name = Column(String(255), nullable=False, unique=True)
    source = Column(String(255), default=0)
    type = Column(String(255), default=0)
    nums = Column(Integer, default=0)
    u_num = Column(Integer,default=0)

    def __repr__(self):
        return "<EventAction:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 海报分享统计信息
class Product(BaseModel):
    __tablename__ = 'analysis_product'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    product_id = Column(Integer, nullable=False, unique=True)
    view = Column(Integer, default=0)
    cart = Column(Integer, default=0)
    order = Column(Integer, default=0)

    def __repr__(self):
        return "<Product:date={date},poster_id={poster_id},product_id={product_id}>".format(date=self.date,
                                                                                            poster_id=self.poster_id,
                                                                                            product_id=self.product_id)


# 优惠券统计信息
class Coupon(BaseModel):
    __tablename__ = 'analysis_coupon'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    coupon_id = Column(Integer, nullable=False, unique=True)
    view = Column(Integer, default=0)
    fetch = Column(Integer, default=0)
    use = Column(Integer, default=0)

    def __repr__(self):
        return "<Coupon:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 海报名片统计信息
class Card(BaseModel):
    __tablename__ = 'analysis_card'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=True)
    card_id = Column(Integer, nullable=False, unique=True)
    view = Column(Integer, default=0)
    favorite = Column(Integer, default=0)

    def __repr__(self):
        return "<PageView:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


# 海报分享统计信息
class ProductAddr(BaseModel):
    __tablename__ = 'analysis_product_addr'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    poster_id = Column(Integer, nullable=False, unique=False)
    product_id = Column(Integer, nullable=False, unique=True)
    v_addr = Column(String(1024), default="")
    o_addr = Column(String(1024), default="")

    def __repr__(self):
        return "<ProductAddr:date={date},poster_id={poster_id}>".format(date=self.date, poster_id=self.poster_id)


class WebsiteSummary(BaseModel):
    __tablename__ = 'analysis_website_summary'
    id = Column(Integer, primary_key=True)
    skin = Column(Integer, default=0)
    template = Column(Integer, default=0)
    poster = Column(Integer, default=0)
    approve_poster = Column(Integer, default=0)
    pv = Column(Integer, default=0)
    uv = Column(Integer, default=0)
    vv = Column(Integer, default=0)
    ip = Column(Integer, default=0)
    user = Column(Integer, default=0)
    update_at = Column(TIMESTAMP(True))

    def __repr__(self):
        return "<WebsiteSummary:id={id},".format(id=self.id)


class WebsiteUserSummary(BaseModel):
    __tablename__ = 'analysis_website_user_summary'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    user = Column(Integer, default=0)
    pv = Column(Integer, default=0)
    login = Column(Integer, default=0)
    register = Column(Integer, default=0)
    sign = Column(Integer, default=0)
    live_rate = Column(Float(precision=2), default=0.00)
    register_rate = Column(Float(precision=2), default=0.00)
    addr = Column(String(1024), default='')

    def __repr__(self):
        return "<WebsiteUserSummary:date={date},".format(date=self.date)


class WebsitePosterSummary(BaseModel):
    __tablename__ = 'analysis_website_poster_summary'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    poster = Column(Integer, default=0)
    new = Column(Integer, default=0)
    published = Column(Integer, default=0)
    approved = Column(Integer, default=0)
    refused = Column(Integer, default=0)
    publish_rate = Column(Float(precision=2), default=0.00)
    approve_rate = Column(Float(precision=2), default=0.00)

    def __repr__(self):
        return "<WebsitePosterSummary:date={date},".format(date=self.date)
