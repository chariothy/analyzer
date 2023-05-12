APP_NAME = 'mschange'
from os import path
from utils import AnalyzerUtil, tmp_env
au = AnalyzerUtil(APP_NAME)

from premailer import transform
import pandas as pd
from pandas import DataFrame
from pybeans import utils as pu
from datetime import timedelta
from pandas.core.frame import DataFrame
import json

TOPN = 10

@pu.benchmark
def query_fund(where:str=''):
    '''
1. 查出最近两次爬得的基金业绩
    '''
    sql = f'''
SELECT r.name fund_name, r.code, r.npv, r.asset, r.manager, r.return_date, v.favorite
, class_id, cash_p AS cash, stock_p AS stock, bond_p AS bond, other_p AS other
FROM spd_ms_fund_return r, v_latest_fund v
WHERE r.code=v.code AND (
SELECT COUNT(r1.id)
FROM spd_ms_fund_return r1
WHERE r.code=r1.code AND r.return_date >= r1.return_date
) <=2
{f'AND ({where})' if where else ''}
ORDER BY r.code, r.return_date DESC
     '''
    df_fund = pd.read_sql(sql, au.session.bind, parse_dates=['return_date'])
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
    
    def manager_desc(m):
        if not m: return m
        else:
            iter_current_manager = filter(lambda m: not m['Leave'], json.loads(m))
            iter_formatted = map(lambda m: f"{m['ManagerName']}({m['ManagementTime']})", iter_current_manager)
            return ', '.join(iter_formatted)
    df_fund['manager_desc'] = ''
    df_fund.manager_desc = df_fund.manager.apply(manager_desc)
    
    def manager_id(m):
        if not m: return m
        else:
            iter_current_manager = filter(lambda m: not m['Leave'], json.loads(m))
            iter_formatted = map(lambda m: m['ManagerId'], iter_current_manager)
            return set(iter_formatted)
    df_fund['manager_id'] = None
    df_fund.manager_id = df_fund.manager.apply(manager_id)
    return df_fund


def calc_fund_change(df: DataFrame):
    df['pre_npv'] = 0
    df['pre_asset'] = 0
    df['pre_manager'] = ''
    df['npv_change'] = 0
    df['asset_change'] = 0
    df['manager_change'] = False
    code_array = df['code'].unique()
    fund_changes = []
    for code in code_array:
        code_df = df[df.code==code].copy()
        if code_df.code.count() >= 2:
            cur = code_df.iloc[0].copy() # 避免Returning a view versus a copy警告
            pre = code_df.iloc[1]
            
            cur_npv = cur.npv
            pre_npv = pre.npv
            cur.pre_npv = pre_npv
            cur.npv_change = (cur_npv - pre_npv) * 100 / pre_npv
            
            cur_asset = cur.asset
            pre_asset = pre.asset
            cur.pre_asset = pre_asset
            cur.asset_change = (cur_asset - pre_asset) * 100 / pre_asset
            
            cur_manager = cur.manager_id
            pre_manager = pre.manager_id
            cur.pre_manager = pre.manager_desc
            cur.manager_change = cur_manager != pre_manager
            fund_changes.append(cur)

    change_df = pd.DataFrame(fund_changes)
    return change_df


def top_npv_change(df: DataFrame, asc=True):
    return df.sort_values(by=['npv_change'], ascending=asc).head(TOPN)


def top_asset_change(df: DataFrame, asc=True):
    df = df[df.asset_change != 0]
    return df.sort_values(by=['asset_change'], ascending=asc).head(TOPN)


def manager_changed(df: DataFrame, asc=True):
    return df[df.manager_change==True]


def report_change(change_df: DataFrame=None):
    if change_df is None:
        fund_df = query_fund()
        change_df = calc_fund_change(fund_df)
    types = ['stock', 'bond']
    for type in types:
        if type == 'stock':
            fund_df_type = change_df[change_df.stock > change_df.bond]
            title = f'最新偏股型基金变动报告({au.env()})'
        else:
            fund_df_type = change_df[change_df.stock <= change_df.bond]
            title = f'最新偏债型及其他基金变动报告({au.env()})'
        if not au.is_prod():
            au.debug(fund_df_type)
        template = tmp_env.get_template('mschange.html')
        html = template.render(dict(
            npv_inc_df = top_npv_change(fund_df_type, False),
            npv_dec_df = top_npv_change(fund_df_type),
            asset_inc_df = top_asset_change(fund_df_type, False),
            asset_dec_df = top_asset_change(fund_df_type),
            manager_change_df = manager_changed(fund_df_type)
        ))
        if au.is_prod():
            au.send_email(title, html_body=transform(html), to_addrs=au['report_to'])
            html_path = path.join('/www',f'fund-latest-{type}.html')
            whole_html = f'''<!DOCTYPE html>
<html>
<head> 
<meta charset="utf-8"> 
<title>{type}</title>
</head>
{html}
</html>'''
            with open(html_path, mode='w') as f:
                f.write(whole_html)
        else:
            html_path = path.join(path.dirname(path.dirname(__file__)),'logs',f'fund-{pu.today()}-{type}.html')
            with open(html_path, mode='w') as f:
                f.write(html)
            import webbrowser
            webbrowser.open(html_path)
    

if __name__ == "__main__":
    report_change()