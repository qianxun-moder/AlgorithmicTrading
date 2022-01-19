import sys

sys.path.append('...')
from data.db.mysql import *
# from sklearn.feature_selection
import pandas as pd
import json


class DataPreprocess(object):

    def __init__(self, start_date=None, end_date=None, db=None):
        self.start_date = start_date
        self.end_date = end_date
        self.db = db



    def read_sql(self, table_name=None, start_date=None, end_date=None):
        sql = f"select * from {table_name} where trade_date between {start_date} and {end_date}"

        return sql

    def stocks_preprocess(self,
                          columns=['ts_code', 'area', 'industry', 'market', 'exchange', 'list_status', 'list_date',
                                   'delist_date', 'is_hs']):
        sql = f"select {','.join(columns)} from stocks_basinfo_cn"

        stocks = self.db.read(read_sql=sql)

        stocks['area'].fillna('N', inplace=True)
        stocks['industry'].fillna('N', inplace=True)


    def base_data_combine(self):
        pass


if __name__ == '__main__':
    db_config_file = '../../db_config.json'
    with open(db_config_file) as f:
        db_cfg = json.load(f)
    db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
    tt = DataPreprocess(db=db)

    tt.stocks_preprocess()

    print('..')