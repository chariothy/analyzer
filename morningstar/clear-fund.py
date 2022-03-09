
from model import FundManager, MsFundBase, MsCategory, MsFundAnnual, MsFundPortfolio, MsFundReturn, MsFundRating, MsFundRank, query_fund
import sys
sys.path.append('../..')
from utils import SpiderUtil
su = SpiderUtil('CLEAR_FUND')


def remove_orphan_fund():
    try:
        su.session.execute('''DELETE FROM spd_ms_fund_portfolio
WHERE id IN (
SELECT r.id
FROM spd_ms_fund_portfolio r
left join spd_ms_fund_base b ON b.CODE=r.CODE
WHERE b.CODE IS NULL
)''')
        su.session.execute('''DELETE FROM spd_ms_fund_rating
WHERE id IN (
SELECT r.id
FROM spd_ms_fund_rating r
left join spd_ms_fund_base b ON b.CODE=r.CODE
WHERE b.CODE IS NULL
)''')
        su.session.execute('''DELETE FROM spd_ms_fund_return
WHERE id IN (
SELECT r.id
FROM spd_ms_fund_return r
left join spd_ms_fund_base b ON b.CODE=r.CODE
WHERE b.CODE IS NULL
)''')
        su.session.execute('''DELETE FROM spd_ms_fund_annual
WHERE id IN (
SELECT r.id
FROM spd_ms_fund_annual r
left join spd_ms_fund_base b ON b.CODE=r.CODE
WHERE b.CODE IS NULL
)''')
        su.session.commit()
        su.I(f'孤儿基金已经删除')
    except Exception as ex:
        su.session.rollback()
        su.F('数据库错误', ex)
        
        
def remove_money_fund():
    try:
        su.session.execute('''DELETE FROM spd_ms_fund_base WHERE CODE IN (
            SELECT b.code 
            FROM spd_ms_category c, spd_ms_fund_base b 
            WHERE b.category_id=c.class_id AND c.class_id='PGSZ04'
        )''')
        su.session.commit()
        su.I('货币基金已经删除')
    except Exception as ex:
        su.session.rollback()
        su.F('数据库错误', ex)
        
        
def remove_old_fund():
    import arrow
    year_ago = arrow.now().shift(years=-1).format('YYYY-MM-DD')
    try:
        su.session.execute(f'''DELETE FROM spd_ms_fund_base WHERE CODE IN (
            SELECT DISTINCT b.code 
            FROM spd_ms_fund_base b, spd_ms_fund_rating r, spd_ms_fund_return e, spd_ms_fund_portfolio p
WHERE b.CODE=r.CODE AND b.CODE=e.CODE AND b.CODE=p.CODE
AND r.rating_date = (SELECT max(rating_date) from spd_ms_fund_rating WHERE CODE=b.code)
AND e.return_date = (SELECT max(return_date) FROM spd_ms_fund_return WHERE CODE=b.code)
AND p.portfolio_date = (SELECT MAX(portfolio_date) FROM spd_ms_fund_portfolio WHERE CODE=b.CODE)
AND (e.return_date < '{year_ago}' and r.rating_date < '{year_ago}' and p.portfolio_date < '{year_ago}')
        )''')
        su.session.commit()
        su.I(f'{year_ago}之前的老基金已经删除')
    except Exception as ex:
        su.session.rollback()
        su.F('数据库错误', ex)
        
        
if __name__ == '__main__':
    remove_money_fund()
    remove_old_fund()
    remove_orphan_fund()