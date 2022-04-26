APP_NAME = 'msrank'
import imp
from utils import AnalyzerUtil, tmp_env
au = AnalyzerUtil(APP_NAME)

from premailer import transform
import pandas as pd
from pandas import DataFrame
from pybeans import utils as pu

TOP = 5


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
SELECT b.code AS code, b.name AS fund_name, c.name AS cat_name, c.banchmark AS banchmark, b.class_id AS class_id
, b.reg_date AS reg_date, b.favorite
, round(json_value(b.fee, '$.Management')+json_value(b.fee, '$.Custodial')+json_value(b.fee, '$.Distribution'),2) AS fee, f.free_at
, r.y2_risk_rating AS y2riskr, r.y3_risk_rating AS y3riskr, r.y5_risk_rating AS y5riskr
, r.y3_ms_rating AS y3msr, r.y5_ms_rating AS y5msr
, r.y3_ms_risk AS y3risk, r.y5_ms_risk AS y5risk
, r.y3_std AS y3std, r.y5_std AS y5std
, r.y3_sharp AS y3sharp, r.y5_sharp AS y5sharp
, r.alpha_ind AS alphai, r.beta_ind AS betai, r.r2_ind AS r2i
, r.worst_m3_return AS w3r, r.worst_m6_return AS w6r, r.rating_date AS rating_date
, e.m1_return AS m1r, e.m3_return AS m3r, e.m6_return AS m6r
, e.ytd_return AS ytdr, e.y1_return AS y1r, e.y2_return AS y2r, e.y3_return AS y3r, e.y5_return AS y5r
, e.return_date AS return_date
, e.asset AS asset, p.cash_p AS cash_p, p.stock_p AS stock_p, p.bond_p AS bond_p, p.other_p AS other_p, p.top_stock_w AS top_stockw, p.portfolio_date AS portfo_date 
, m.managers
FROM spd_ms_category c, spd_ms_fund_base b, spd_ms_fund_rating r, spd_ms_fund_return e, spd_ms_fund_portfolio p
, (SELECT b.code AS CODE, GROUP_CONCAT(CONCAT(jt.manager, ' (', during,')') SEPARATOR ', ') AS managers
FROM spd_ms_fund_base b, json_table(b.manager, '$[*]' columns(
	manager VARCHAR(10) path '$.ManagerName',
	during VARCHAR(10) path '$.ManagementTime',
   is_leave boolean path '$.Leave'
	)) as jt
WHERE jt.is_leave=0 GROUP BY b.code) AS m
, (SELECT b.code, free_at
FROM spd_ms_fund_base b, json_table(b.fee, '$.Redemption[*]' columns(
	free_at VARCHAR(50) path '$.key[0]',
	fee_r VARCHAR(10) path '$.value'
	)) as jt
where fee_r='0.00%') as f
WHERE b.category_id=c.class_id AND b.CODE=r.CODE AND b.CODE=e.CODE AND b.CODE=p.CODE and b.code=m.code and b.code=f.code
AND r.rating_date = (SELECT max(rating_date) from spd_ms_fund_rating WHERE CODE=b.code)
AND e.return_date = (SELECT max(return_date) FROM spd_ms_fund_return WHERE CODE=b.code)
AND p.portfolio_date = (SELECT MAX(portfolio_date) FROM spd_ms_fund_portfolio WHERE CODE=b.CODE)
{f'AND ({where})' if where else ''}
    '''
    df_fund = pd.read_sql(sql, au.session.bind, parse_dates=['rating_date', 'return_date'])
    index_to_del = []
    for index, row in df_fund.iterrows():
        fund_name = row.fund_name
        # 去除C类基金
        if 'C' in fund_name:
            fund_name_a = row.fund_name.replace('C', 'A')
            if df_fund[df_fund.fund_name == fund_name_a].code.count() > 0:
                index_to_del.append(index)
    #au.I(index_to_del)
    return df_fund.drop(index_to_del)


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
    # 将5年夏普值降序，5年标准差升序，6个月最差收益降序，进行综合排名
    # 选出每类中的前8名，再将这8名按最近6个月收益值升序排序
    # 高sharp 低std 高alpha 高远5t/近6m收益 低费率 低asset
    cate_array = df['cat_name'].unique()
    top_funds = []
    rank_conditions = dict(
        fee = dict(asc=True, weight=0.1),
        asset = dict(asc=True, weight=0.1),
        m6r = dict(asc=False, weight=0.6),
        alphai = dict(asc=False, weight=0.5),
        y5std = dict(asc=True, weight=0.8),
        y5sharp = dict(asc=False, weight=1)
    )
    for cat_name in cate_array:
        cat_df = df[df.cat_name==cat_name].copy()
        cat_df['fund_rank5'] = 0
        for col in rank_conditions:
            condition = rank_conditions[col]
            percentile = f'{col}_pct5'
            cat_df[percentile] = cat_df[col].rank(method='min', ascending=condition['asc'])
            cat_df['fund_rank5'] += cat_df[percentile] * condition['weight']
            
        ret_date = cat_df.iloc[0].return_date.strftime('%Y%m%d')
        rat_date = cat_df.iloc[0].rating_date.strftime('%Y%m%d')
        top_cat_df = cat_df.head(TOP).sort_values(by=['m6r'])
        top_funds.append(top_cat_df)

    all_top_funds = pd.concat(top_funds)
    #all_top_funds.to_csv(f'c:/temp/fund/all_top_funds_{ret_date}_{rat_date}.csv', encoding='utf_8_sig')
    #export_fund(all_top_funds)
    return all_top_funds


def report_top_rank():
    top_fund_df = query_fund()
    rank_fund_df = rank_fund(top_fund_df)
    au.D(rank_fund_df)
    rank_fund_group_df = rank_fund_df.groupby('cat_name')
    #au.D(rank_fund_group_df.groups.keys())
    data = dict(
        df = rank_fund_group_df
    )
    template = tmp_env.get_template('msrank.html')
    html = template.render({'data': data})
    #au.D(html)
    html = transform(html)
    au.send_email(f'最新TOP{TOP}基金报告({au.env()})', html_body=html)
    

if __name__ == "__main__":
    report_top_rank()