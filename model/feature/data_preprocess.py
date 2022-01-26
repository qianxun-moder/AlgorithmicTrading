import sys

sys.path.append('...')
from data.db.mysql import *
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import pandas as pd
import numpy as np
import json


class DateTrf(object):

    def fit(self, X=None, y=None):
        pass

    def transform(self, data=None):
        data = pd.DataFrame(data)[0]
        data = pd.to_datetime(data)
        month = data.dt.month / 12
        date = data.dt.day / 32
        week = data.dt.week / 53
        day_of_week = data.dt.day_of_week / 7
        day_of_year = data.dt.day_of_year / 366
        is_leep_year = data.dt.is_leap_year.astype('int')

        return pd.concat([month, date, week, day_of_week, day_of_year, is_leep_year], axis=1).to_numpy()


class TimeTrf(object):
    def fit(self, X=None, y=None):
        pass

    def transform(self, data=None):
        data = pd.DataFrame(data)[0]
        data = pd.to_datetime(data)
        hour = data.dt.hour / 24
        minute = data.dt.minute / 60

        return pd.concat([hour, minute], axis=1).to_numpy()


class DataPrep(object):

    def __init__(self, start_date=None, end_date=None, db_cfg=None):
        self.start_date = start_date
        self.end_date = end_date

        with open(db_cfg) as f:
            db_cfg = json.load(f)
        self.db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
        self.trf_funcs = {}
        self.ft_cols = []
        self.tffm = {'OneHot': OneHotEncoder, 'date': DateTrf, 'std_scal': StandardScaler, 'time': TimeTrf}
        self.label_ths = {'y1': 1.05, 'y2': 1.05, 'y3': 1.05}

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
        funcs = ['date'] + ['std_scal'] * 9

        self.fit(data=dl, data_name='daily', cols=cols, funcs=funcs)
        tfdl = self.transform(data=dl, data_name='daily', cols=cols, remain_col=['ts_code', 'trade_date'])

        return tfdl

    def suspend(self, start_date=None, end_date=None):
        '''
        时间字段大量为空，暂时不用
        :param start_date:
        :param end_date:
        :return:
        '''
        sql = f"select * from suspend_cn where trade_date between '{start_date}' and '{end_date}'"
        sus = self.db.read(sql)

        cols = []

    def moneyflow(self, start_date=None, end_date=None):
        sql = f"select * from moneyflow_cn where trade_date between '{start_date}' and '{end_date}'"
        mf = self.db.read(sql)

        cols = ['buy_sm_vol', 'buy_sm_amount', 'sell_sm_vol',
                'sell_sm_amount', 'buy_md_vol', 'buy_md_amount', 'sell_md_vol',
                'sell_md_amount', 'buy_lg_vol', 'buy_lg_amount', 'sell_lg_vol',
                'sell_lg_amount', 'buy_elg_vol', 'buy_elg_amount', 'sell_elg_vol',
                'sell_elg_amount', 'net_mf_vol', 'net_mf_amount', 'trade_count']
        funcs = ['std_scal'] * 19

        self.fit(data=mf, data_name='moneyflow', cols=cols, funcs=funcs)
        tfmf = self.transform(data=mf, data_name='moneyflow', cols=cols, remain_col=['ts_code', 'trade_date'])

        return tfmf

    def limit_list(self, start_date=None, end_date=None):
        sql = f"select * from limit_list_cn where trade_date between {start_date} and {end_date}"
        ls = self.db.read(sql)

        cols = ['first_time', 'last_time', 'close', 'pct_chg', 'amp', 'fc_ratio', 'fl_ratio', 'fd_amount', 'open_times',
                'strth', 'limit']
        funcs = ['time'] * 2 + ['std_scal'] * 8 + ['OneHot']
        self.fit(data=ls, data_name='limit_list', cols=cols, funcs=funcs)

        trls = self.transform(data=ls, data_name='limit_list', cols=cols, remain_col=['ts_code', 'trade_date'])

        return trls

    def merge_data(self):

        stock_data = self.stocks()
        dl = self.daily(start_date='20220111', end_date='20220115')
        mf = self.moneyflow(start_date='20220111', end_date='20220115')
        ls = self.limit_list(start_date='20220111', end_date='20220115')

        mgd = pd.merge(left=dl, right=stock_data, left_on='ts_code', right_on='ts_code', how='left')
        mgd = pd.merge(left=mgd, right=mf, left_on=['ts_code', 'trade_date'], right_on=['ts_code', 'trade_date'],
                       how='left')
        mgd = pd.merge(left=mgd, right=ls, left_on=['ts_code', 'trade_date'], right_on=['ts_code', 'trade_date'],
                       how='left')

        self.mgd = mgd
        return mgd

    def get_label(self):
        self.mgd['post1'] = self.mgd.sort_values('trade_date').groupby('ts_code')['daily-close-0'].shift(1)
        self.mgd['post3'] = self.mgd.sort_values('trade_date').groupby('ts_code')['daily-close-0'].shift(3)
        self.mgd['post5'] = self.mgd.sort_values('trade_date').groupby('ts_code')['daily-close-0'].shift(5)
        self.mgd['y1'] = self.mgd['post1'] / self.mgd['daily-close-0']
        self.mgd['y2'] = self.mgd['post3'] / self.mgd['daily-close-0']
        self.mgd['y3'] = self.mgd['post5'] / self.mgd['daily-close-0']

        self.cp_col = ['ts_code']
        self.data_col = ['trade_date']
        self.y_cols = ['post1', 'post2', 'post3', 'y1', 'y2', 'y3']
        self.ft_cols = list(set(self.mgd.columns.tolist()) - set(self.cp_col + self.data_col + self.y_cols))

    def get_train_data(self, dtype='y1'):

        self.merge_data()
        self.get_label()

        tmp = self.mgd[~self.mgd[dtype].isna()]
        X = tmp[self.ft_cols].to_numpy()
        Y = (tmp[dtype] > self.label_ths[dtype]).astype('int').to_numpy()

        return X, Y


if __name__ == '__main__':
    db_config_file = '../../db_config.json'
    with open(db_config_file) as f:
        db_cfg = json.load(f)
    # db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
    tt = DataPrep(db_cfg=db_config_file)

    tt.get_train_data('y1')
    print('..')
