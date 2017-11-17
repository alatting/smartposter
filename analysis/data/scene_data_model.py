#=================================================================================
# 新版海报数据统计分析
# create by hw 2017/11/17
#=================================================================================

from sqlalchemy import Column, Integer, String, Date, Float, TIMESTAMP
from . import BaseModel


class SceneViewInfo(BaseModel):
    __tablename__ = 'scene_view_info'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    scene_id = Column(Integer, default=0)
    pv = Column(Integer, default=0)
    uv = Column(Integer, default=0)
    share = Column(Integer, default=0)
    address = Column(String,default="")
    def __repr__(self):
        return "<SceneViewInfo:date={date},".format(date=self.date)
