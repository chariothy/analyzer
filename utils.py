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
tmp_env.filters['free'] = lambda val: '年' if float(val) < 4 else '天'

def val(x):
    color = 'red' if x <= 0 else 'black'
    return f'<span style="color: {color}">{round(x,2)}</span>'
tmp_env.filters['val'] = val

def sharp(x):
    color = 'blue' if x > 1 else 'black'
    return f'<span style="color: {color}">{round(x,2)}</span>'
tmp_env.filters['sharp'] = sharp

def rating(r):
    r = int(r)
    return (r * '★').ljust(5, '☆')
tmp_env.filters['rating'] = rating

def split(s):
    if s:
        return '<br>'.join(map(lambda s: '- '+s, s.split(',')))
    else:
        return ''
tmp_env.filters['split'] = split

def updown(x):
    color = 'blue' if x > 0 else 'red'
    sig = '↑' if x > 0 else '↓'
    return f'<span style="color: {color}">{sig} {round(x, 2)} </span>'
tmp_env.filters['updown'] = updown


def industry(fund):
    s = ''
    if fund['stock'] >= fund['bond']:
        bond_cash = fund['bond'] + fund['cash']
        if bond_cash < 0.01:
            s1 = '全股票'
        else:
            s1 = round(fund['stock'] / bond_cash, 2)
        s += f'<span class="stock-ratio">〓股票÷(债券+现金) = {s1}</span>'
        s += f'<br><span class="top-stock">{split(fund["top_stock"])}</span>'
    else:
        s += f'<span class="top-bond">{split(fund["top_bond"])}</span>'
    return s
tmp_env.filters['industry'] = industry

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
    
    def is_prod(self):
        return self.env() == 'prod'