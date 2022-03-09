# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, Float, String, TIMESTAMP, text, JSON, FLOAT
from sqlalchemy.dialects.mysql import INTEGER, TINYINT, VARCHAR  
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import and_
from sqlalchemy.sql.functions import func

Base = declarative_base()
metadata = Base.metadata

def to_dict(self):
    return {c.name: getattr(self, c.name, None)
        for c in self.__table__.columns}
Base.to_dict = to_dict

from schema.morningstar import MsFundRating, MsFundReturn, MsFundPortfolio, MsFundBase, MsCategory


class FundManager(Base):
    __tablename__ = 'spd_fund_manager'
    __table_args__ = {'comment': '基金经理'}

    id = Column(INTEGER(11), primary_key=True, comment='ID')
    name = Column(VARCHAR(10), nullable=False, comment='姓名')
    ttfund_id = Column(VARCHAR(10), nullable=False, comment='天天基金编号')
    start_date = Column(String(10, 'utf8mb4_unicode_ci'), nullable=False, comment='任职起始日期')
    max_fund = Column(VARCHAR(10), nullable=False, comment='规模最大基金')
    max_active = Column(INTEGER(11), nullable=False, comment='最大基金是否在任')
    max_scale = Column(DECIMAL(10, 2), nullable=False, comment='最大基金规模(亿)')
    max_days = Column(INTEGER(11), nullable=False, comment='最大基金任职天数')
    max_return = Column(DECIMAL(8, 2), nullable=False, comment='最大基金年化回报')
    oldest_fund = Column(VARCHAR(10), nullable=False, comment='任职最久基金')
    oldest_active = Column(INTEGER(11), nullable=False, comment='最久基金是否在任')
    oldest_scale = Column(DECIMAL(10, 2), nullable=False, comment='最久基金规模(亿)')
    oldest_days = Column(INTEGER(11), nullable=False, comment='最久基金任职天数')
    oldest_return = Column(DECIMAL(8, 2), nullable=False, comment='最久基金年化回报')
    best_fund = Column(VARCHAR(10), nullable=False, comment='回报最优基金')
    best_active = Column(INTEGER(11), nullable=False, comment='最优基金是否在任')
    best_scale = Column(DECIMAL(10, 2), nullable=False, comment='最优基金规模(亿)')
    best_days = Column(INTEGER(11), nullable=False, comment='最优基金任职天数')
    best_return = Column(DECIMAL(8, 2), nullable=False, comment='最优基金年化回报')
    
    
def query_fund(session, filter_by=None, rating_date:str=None, return_date:str=None, portfolio_date:str=None):
    if not rating_date:
        rating_date = session.query(func.max(MsFundRating.rating_date))
    if not return_date:
        return_date = session.query(func.max(MsFundReturn.return_date))
    if not portfolio_date:
        portfolio_date = session.query(func.max(MsFundPortfolio.portfolio_date))
    query = session \
        .query(MsFundBase.code.label('code'), MsFundBase.name.label('fund_name'), MsCategory.name.label('cat_name') \
            , MsFundRating.worst_m3_return.label('w3r'), MsFundRating.worst_m6_return.label('w6r') \
            , MsFundRating.y3_ms_risk.label('y3risk'), MsFundRating.y5_ms_risk.label('y5risk') \
            , MsFundRating.y3_std.label('y3std'), MsFundRating.y5_std.label('y5std') \
            , MsFundRating.y3_sharp.label('y3sharp'), MsFundRating.y5_sharp.label('y5sharp') \
            , MsFundRating.alpha_ind.label('alpha'), MsFundRating.beta_ind.label('beta'), MsFundRating.r2_ind.label('r2') \
            , MsFundRating.rating_date.label('rating_date') \
            , MsFundReturn.m1_return.label('m1r') \
            , MsFundReturn.m3_return.label('m3r'), MsFundReturn.m6_return.label('m6r') \
            , MsFundReturn.y3_return.label('y3r'), MsFundReturn.y5_return.label('y5r') \
            , MsFundReturn.return_date.label('return_date'), MsFundReturn.asset.label('asset') \
            , MsFundPortfolio.cash_p.label('cash_p'), MsFundPortfolio.stock_p.label('stock_p') \
            , MsFundPortfolio.bond_p.label('bond_p'), MsFundPortfolio.other_p.label('other_p') \
            , MsFundPortfolio.top_bond_w.label('top_bond_w'), MsFundPortfolio.top_stock_w.label('top_stock_w') \
        )\
        .join(MsCategory, MsFundBase.category_id == MsCategory.class_id) \
        .join(MsFundRating, and_(MsFundBase.code == MsFundRating.code, MsFundRating.rating_date == rating_date)) \
        .join(MsFundReturn, and_(MsFundBase.code == MsFundReturn.code, MsFundReturn.return_date == return_date)) \
        .join(MsFundPortfolio, and_(MsFundBase.code == MsFundPortfolio.code, MsFundPortfolio.portfolio_date == portfolio_date))
    if filter_by is not None:
        query = query.filter(filter_by)
    return query