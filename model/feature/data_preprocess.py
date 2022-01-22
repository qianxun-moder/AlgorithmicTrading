import sys

sys.path.append('...')
from data.db.mysql import *
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import pandas as pd
import numpy as np
import json


class TimeTrf(object):

    def fit(self, X=None, y=None):
        pass

    def transform(self, data=None):
        data = pd.DataFrame(data)[0]
        data = pd.to_datetime(data)
        month = data.dt.month/12
        date = data.dt.day/32
        week = data.dt.week/53
        day_of_week = data.dt.day_of_week/7
        day_of_year = data.dt.day_of_year/366
        is_leep_year = data.dt.is_leap_year.astype('int')

        return pd.concat([month, date, week, day_of_week, day_of_year, is_leep_year], axis=1).to_numpy()


class DataPrep(object):

    def __init__(self, start_date=None, end_date=None, db=None):
        self.start_date = start_date
        self.end_date = end_date
        self.db = db
        self.trf_funcs = {}

        self.tffm = {'OneHot': OneHotEncoder, 'time': TimeTrf, 'std_scal': StandardScaler}

    def read_sql(self, table_name=None, start_date=None, end_date=None):
        sql = f"select * from {table_name} where trade_date between {start_date} and {end_date}"

        return sql

    def fit(self, data=None, data_name=None, cols=None, funcs=None):
        self.trf_funcs.update({data_name: {}})
        for c, f in zip(cols, funcs):
            tmp = self.tffm[f]()
            tmp.fit(data[c].to_numpy().reshape(-1, 1))
            self.trf_funcs[data_name].update({c: tmp})

    def transform(self, data=None, data_name=None, cols=None, remain_col=None):
        tr_data = pd.DataFrame([])
        for c in cols:
            td = self.trf_funcs[data_name][c].transform(data[c].to_numpy().reshape(-1, 1))
            if type(td).__name__ == 'csr_matrix':
                td = td.toarray()
            cln = [data_name + '-' + c + '-' + str(i) for i in range(td.shape[1])]
            tpd = pd.DataFrame(td, columns=cln)
            tr_data = pd.concat([tr_data, tpd], axis=1)
        tr_data = pd.concat([data[remain_col], tr_data], axis=1)
        return tr_data

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

        self.fit(data=stocks, data_name='stocks', cols=['area', 'industry', 'market', 'exchange', 'is_hs'],
                 funcs=['OneHot'] * 5)
        trf_data = self.transform(data=stocks, data_name='stocks',
                                  cols=['area', 'industry', 'market', 'exchange', 'is_hs'],
                                  remain_col=['ts_code'])

        return trf_data

    def cal(self, columns=[]):
        '''
        是否开盘数据，模型训练不需要
        :param columns:
        :return:
        '''
        sql = f"select {','.join(columns)} from trade_cal_cn"
        cals = self.db.read(read_sql=sql)

        print('..')

    def daily(self, start_date=None, end_date=None):

        sql = f"select * from daily_cn where trade_date between '{start_date}' and '{end_date}'"
        dl = self.db.read(sql)

        cols = ['trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        funcs = ['time'] + ['std_scal'] * 9

        self.fit(data=dl, data_name='daily', cols=cols, funcs=funcs)
        tfdl = self.transform(data=dl, data_name='daily', cols=cols, remain_col=['ts_code'])

        return tfdl

    def combine_transform(self):

        '''

        :return:
        '''
        pass


if __name__ == '__main__':
    db_config_file = '../../db_config.json'
    with open(db_config_file) as f:
        db_cfg = json.load(f)
    db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
    tt = DataPrep(db=db)

    stock_data = tt.stocks()
    cal_data = tt.daily(start_date='20220111', end_date='20220115')

    print('..')
