import sys

sys.path.append('...')
from data.db.mysql import *
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import json


class DataPreprocess(object):

    def __init__(self, start_date=None, end_date=None, db=None):
        self.start_date = start_date
        self.end_date = end_date
        self.db = db
        self.trans_func_l = []

    def read_sql(self, table_name=None, start_date=None, end_date=None):
        sql = f"select * from {table_name} where trade_date between {start_date} and {end_date}"

        return sql

    def stocks(self, columns=['ts_code', 'area', 'industry', 'market', 'exchange', 'list_date', 'is_hs']):
        '''
        :param columns:
            20220120：去除 'list_status',因只有一个取值。
            20220120：去除 'delist_date',因全为空
        :return:
        '''
        sql = f"select {','.join(columns)} from stocks_basinfo_cn"

        stocks = self.db.read(read_sql=sql)

        stocks.fillna('Nan', inplace=True)

    def cal(self, columns=[]):

        sql = f"select {','.join(columns)} from stocks_basinfo_cn"

    def combine_transform(self):
        '''
        整合数据，并进行编码，标准化等特征操作
        :return:
        '''

        stocks = 0
        area_pre = OneHotEncoder().fit(stocks['area'])
        stocks['area'] = area_pre.tranform(stocks['area'])
        self.prep_func_l.append({})


if __name__ == '__main__':
    db_config_file = '../../db_config.json'
    with open(db_config_file) as f:
        db_cfg = json.load(f)
    db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
    tt = DataPreprocess(db=db)

    tt.stocks_preprocess()

    print('..')
