APP_NAME = 'msrank'
from os import path
from utils import AnalyzerUtil, tmp_env
au = AnalyzerUtil(APP_NAME)

from premailer import transform
import pandas as pd
from pandas import DataFrame
from pybeans import utils as pu
from datetime import timedelta

import json

TOP = 5

@pu.benchmark
def query_fund(where:str=''):
    '''

MEMO: 分批查询
from sqlalchemy import create_engine, select

conn = create_engine("DB URL...").connect()
q = select([huge_table])

proxy = conn.execution_options(stream_results=True).execute(q)

while 'batch not empty':  # equivalent of 'while True', but clearer
    batch = proxy.fetchmany(100000)  # 100,000 rows at a time

    if not batch:
        break

    for row in batch:
        # Do your stuff here...

proxy.close()

    TODO:
1. 删除数据库中的货币及封闭，然后运行新版本
1. portfolio sector 另存表格
1. 分离出天天基金的基金经理到另一个爬虫
    '''
    sql = f'''
SELECT code, name AS fund_name, cat_name, banchmark, class_id, reg_date, favorite, can_buy, fee, free_at
, y3_risk_rating AS y3riskr, y5_risk_rating AS y5riskr
, y3_ms_rating AS y3msr, y5_ms_rating AS y5msr, y3_ms_risk AS y3risk, y5_ms_risk AS y5risk
, y3_std AS y3std, y5_std AS y5std, y3_sharp AS y3sharp, y5_sharp AS y5sharp, rating_date
, nm1_return AS m1r, nm1_cat as m1c, m3_return AS m3r, m6_return AS m6r, ytd_return AS ytdr, y2_return AS y2r
, y1_return AS y1r, y1_cat_size AS y1size, y1_cat_rank AS y1rank
, y3_return AS y3r, y3_cat_size AS y3size, y3_cat_rank AS y3rank
, y5_return AS y5r, y5_cat_size AS y5size, y5_cat_rank AS y5rank, return_date
, asset, cash_p AS cash, stock_p AS stock, bond_p AS bond, other_p AS other
, top_stock_w AS top_stockw, top_bond_w AS top_bondw, portfolio_date AS portfo_date 
, manager, top10_stock AS top_stock, top5_bond AS top_bond, industry_sector
FROM v_latest_fund
{f'AND ({where})' if where else ''}
     '''
    df_fund = pd.read_sql(sql, au.session.bind, parse_dates=['rating_date', 'return_date'])
    index_to_del = []
    for index, row in df_fund.iterrows():
        fund_name = row.fund_name
        # 如果存在A类基金，去除B/C类基金
        if 'C' in fund_name:
            fund_name_a = row.fund_name.replace('C', 'A')
            if df_fund[df_fund.fund_name == fund_name_a].code.count() > 0:
                index_to_del.append(index)
        if 'B' in fund_name:
            fund_name_a = row.fund_name.replace('B', 'A')
            if df_fund[df_fund.fund_name == fund_name_a].code.count() > 0:
                index_to_del.append(index)
    #au.I(index_to_del)
    df_fund = df_fund.drop(index_to_del)
    
    def manager(m):
        if not m: return m
        else:
            iter_current_manager = filter(lambda m: not m['Leave'], json.loads(m))
            iter_formatted = map(lambda m: f"{m['ManagerName']}({m['ManagementTime']})", iter_current_manager)
            return ', '.join(iter_formatted)
    df_fund.manager = df_fund.manager.apply(manager)
    
    def sector(s):
        if not s: return s
        else:
            iter_kept_major = filter(lambda s: s['NetAssetWeight']>1, sorted(json.loads(s), key=lambda s: s['NetAssetWeight'], reverse=True))
            shrink_industry = lambda s: s if len(s) <= 10 else f'{s[:8]}...'
            iter_formatted = map(lambda s: f"{shrink_industry(s['IndustryName'])} {s['NetAssetWeight']} / {s['CatAvgWeight']}", list(iter_kept_major)[:3])
            return ', '.join(iter_formatted)
    df_fund.industry_sector = df_fund.industry_sector.apply(sector)
    
    def stock(s):
        if not s: return s
        else:
            iter_kept_major = filter(lambda s: s['Percent']>1, sorted(json.loads(s), key=lambda s: s['Percent'], reverse=True))
            iter_formatted = map(lambda s: f"{s['HoldingName']} {s['Percent']}", list(iter_kept_major)[:3])
            return ', '.join(iter_formatted)
    df_fund.top_stock = df_fund.top_stock.apply(stock)
    
    def bond(s):
        if not s: return s
        else:
            iter_kept_major = filter(lambda s: s['Percent']>1, sorted(json.loads(s), key=lambda s: s['Percent'], reverse=True))
            iter_formatted = map(lambda s: f"{s['HoldingName']}: {s['Percent']}", list(iter_kept_major)[:3])
            return ', '.join(iter_formatted)
    df_fund.top_bond = df_fund.top_bond.apply(bond)

    return df_fund


def export_fund(df:DataFrame, folder='./logs'):
    '''
1。 return cat_rank/cat_size
    '''
    df['fund_name'] = '=HYPERLINK("http://www.morningstar.cn/quicktake/' + df['class_id'] + '","' + df['fund_name'] + '")'
    df['code'] = '=HYPERLINK("http://fund.eastmoney.com/' + df['code'] + '.html","' + df['code'] + '")'
    del df['class_id']
    file_path = f'{folder}/fund-{pu.today()}.csv'
    df.to_csv(file_path, encoding='utf_8_sig')
    print(f'Exported to {file_path}')
    return df


def rank_fund(df:DataFrame):
    # 算法：
    # 思想：长期表现优秀的基金，如果在最近表现偏弱，则其均值回归的概率更大
    # 将5年夏普值降序，5年标准差升序，（删除）6个月最差收益降序，进行综合排名
    # 选出每类中的前8名，再将这8名按最近6个月收益值升序排序
    # 高sharp 低std 高alpha 高远5t/近6m收益 低费率 低asset
    cate_array = df['cat_name'].unique()
    top_funds = []
    rank_conditions = dict(
        fee = dict(asc=True, weight=0.1),
        asset = dict(asc=True, weight=0.1),
        m6r = dict(asc=False, weight=0.6),
        y5std = dict(asc=True, weight=0.8),
        y5sharp = dict(asc=False, weight=1)
    )
    for cat_name in cate_array:
        cat_df = df[df.cat_name==cat_name].copy()
        cat_df = cat_df.sort_values(by=['return_date'], ascending=False)
        last_ret_date = cat_df.iloc[0].return_date.to_pydatetime()
        min_ret_date = last_ret_date - timedelta(days=61)
        cat_df = cat_df[cat_df.return_date >= min_ret_date]
        #cat_df.return_date.describe()

        cat_df['fund_rank5'] = 0
        for col in rank_conditions:
            condition = rank_conditions[col]
            percentile = f'{col}_pct5'
            cat_df[percentile] = cat_df[col].rank(method='min', ascending=condition['asc'])
            cat_df['fund_rank5'] += cat_df[percentile] * condition['weight']
            
        top_cat_df = cat_df.sort_values(by=['fund_rank5']).head(TOP)
        top_funds.append(top_cat_df)

    all_top_funds = pd.concat(top_funds)
    return all_top_funds


def report_top_fund():
    top_fund_df = query_fund()
    rank_fund_df = rank_fund(top_fund_df)
    if not au.is_prod():
        au.D(rank_fund_df)
    rank_fund_group_df = rank_fund_df.groupby('cat_name')
    #au.D(rank_fund_group_df.groups.keys())
    data = dict(
        df = rank_fund_group_df
    )
    template = tmp_env.get_template('msrank.html')
    html = template.render({'data': data})
    html = transform(html)
    if au.is_prod():
        au.send_email(f'最新TOP{TOP}基金报告({au.env()})', html_body=html, to_addrs=au['report_to'])
    else:
        html_path = path.join(path.dirname(path.dirname(__file__)),'logs',f'fund-{pu.today()}.html')
        with open(html_path, mode='w') as f:
            f.write(html)
        import webbrowser
        webbrowser.open(html_path)
    

if __name__ == "__main__":
    report_top_fund()