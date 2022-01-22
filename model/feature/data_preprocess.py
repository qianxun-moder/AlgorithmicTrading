import sys

sys.path.append('...')
from data.db.mysql import *
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import json

class TimeTrf(object):
    def transform(self):



class DataPrep(object):

    def __init__(self, start_date=None, end_date=None, db=None):
        self.start_date = start_date
        self.end_date = end_date
        self.db = db
        self.trf_funcs = {}

    def read_sql(self, table_name=None, start_date=None, end_date=None):
        sql = f"select * from {table_name} where trade_date between {start_date} and {end_date}"

        return sql

    def fit(self, data=None, data_name=None, cols_funcs=None):

        for c, f in cols_funcs:
            if f == 'OneHot':
                self.trf_funcs[data_name] = {c:OneHotEncoder().fit(data[c].to_numpy().reshape(-1, 1))}
            elif f == 'timetrf':
                # todo 时间特征处理
                pass
            else:
                pass
    def transform(self, data=None, data_name=None):
        tr_data = pd.DataFrame([])
        for c in data.columns:
            data = self.trf_funcs[data_name][c].transform(data[c])
            dl = data






    def stocks(self, columns=['ts_code', 'area', 'industry', 'market', 'exchange', 'is_hs']):
        '''
        :param columns:
            20220120：去除 'list_status',因只有一个取值。
            20220120：去除 'delist_date',因全为空
            20220122：去除 'list_date', 时间数据暂时不做处理
        :return:
        '''
        sql = f"select {','.join(columns)} from stocks_basinfo_cn"

        stocks = self.db.read(read_sql=sql)

        stocks.fillna('Nan', inplace=True)

        area_ecd = OneHotEncoder().fit(stocks['area'].to_numpy().reshape(-1, 1))
        area_ecded = area_ecd.transform(stocks['area'].to_numpy().reshape(-1, 1))
        self.prep_func_l['stocks'] = {'area': area_ecd}

        industry_ecd = OneHotEncoder().fit(stocks['industry'].to_numpy().reshape(-1, 1))
        industry_ecded = industry_ecd.transform(stocks['industry'].to_numpy().reshape(-1, 1))
        self.prep_func_l['stocks'] = {'industry': industry_ecd}

        market_ecd = OneHotEncoder().fit(stocks['market'].to_numpy().reshape(-1, 1))
        market_ecded = industry_ecd.transform(stocks['market'].to_numpy().reshape(-1, 1))
        self.prep_func_l['stocks'] = {'market': market_ecd}

        industry_ecd = OneHotEncoder().fit(stocks)

    def cal(self, columns=[]):
        sql = f"select {','.join(columns)} from stocks_basinfo_cn"

    def combine_transform(self):
        '''

        :return:
        '''


if __name__ == '__main__':
    db_config_file = '../../db_config.json'
    with open(db_config_file) as f:
        db_cfg = json.load(f)
    db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
    tt = DataPrep(db=db)

    tt.stocks()

    print('..')
