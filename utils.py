import sys, os
from datetime import datetime as dt
from typing import Pattern
from pybeans import AppTool
import json
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from notify import notify_by_ding_talk

from decimal import Decimal
def my_finalize(thing):
    if thing is None:
        return ''
    elif type(thing) in (float, Decimal):
        return round(thing, 2)
    else:
        return thing
from jinja2 import Environment, FileSystemLoader
tmp_env = Environment(loader=FileSystemLoader(os.getcwd() + '/templates'), finalize=my_finalize)

class AnalyzerUtil(AppTool):
    """
    蜘蛛公用代码
    """
    def __init__(self, spider_name):
        super(AnalyzerUtil, self).__init__(spider_name)
        self._session = None


    @property
    def session(self):
        """
        Lazy loading
        """
        if self._session:
            return self._session
        assert(self['mysql'] is not None)
        DB_CONN = 'mysql+mysqlconnector://{c[user]}:{c[pwd]}@{c[host]}:{c[port]}/{c[db]}?ssl_disabled=True' \
            .format(c=self['mysql'])
        engine = create_engine(
            DB_CONN, 
            pool_size=20, 
            max_overflow=0, 
            echo=self['log.sql'] == 1,
            json_serializer=lambda obj: json.dumps(obj, ensure_ascii=False))
        self._session = sessionmaker(bind=engine)()
        return self._session


    def ding(self, title: str, text: str):
        result = notify_by_ding_talk(self['dingtalk'], title, text)
        self.D(result)
        
        
    def sleep(self, sec=3):
        time.sleep(sec)
        
    def env(self):
        return os.environ.get('ENV')