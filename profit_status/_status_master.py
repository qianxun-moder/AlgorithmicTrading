from data import MysqlOpt
import pandas as pd
import json


class ProfitStatus(object):

    def __init__(self, init_amt=10000, db=None):
        self.init_amt = init_amt
        self.amt = init_amt
        self.columns = ['ts_code', 'trd_amt', 'trd_cnt', 'trd_date', 'amt', 'profit', 'p_ratio', 'st_date']
        self.stocks = pd.DataFrame([], columns=self.columns)
        self.profit = 0.0
        self.db = db

    def update(self, date):
        '''
        收益更新
        :return:
        '''
        stocks = self.stocks['ts_code'].unique().tolist()
        if len(stocks) > 0:
            ts_cdt = '"' + '","'.join(stocks) + '"'
            sql = f'select * from daily_cn where trade_date="{date}" and ts_code in ({ts_cdt})'
            data = self.db.read(sql)
            tmp = pd.merge(left=self.stocks, right=data, how='left', left_on='ts_code', right_on='ts_code')
            tmp['amt'] = tmp['close']
            tmp['profit'] = tmp['amt']-tmp['trd_amt']
            tmp['p_ratio'] = tmp['profit']/tmp['trd_amt']
            tmp['st_date'] = tmp['trade_date']
            self.stocks = tmp[self.columns]
        self.profit = self.stocks['profit'].sum()

    def add_stocks(self, ts_code=None, amt=None, cnt=None, trade_date=None):
        tmp = pd.DataFrame([[ts_code, amt, cnt, trade_date, amt, 0.0, 0.0, trade_date]], columns=self.columns)
        self.stocks = self.stocks.append(tmp)

    def rmove_stocks(self):
        pass

    def get_new_amt(self, ts_cods=[], amt_date=None):
        ts_cdt = '"' + '","'.join(ts_cods) + '"'
        sql = f'select * from daily_cn where statis_date="{amt_date}" and ts_code in ({ts_cdt})'


if __name__ == '__main__':
    db_config_file = '../db_config.json'
    db_name = 'algtrd_db'
    with open(db_config_file) as f:
        db_cfg = json.load(f)
    db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], db_name)

    ps = ProfitStatus(db=db)
    ps.add_stocks(ts_code='000009.SZ', amt=13.9, cnt=10, trade_date='20220113')
    ps.update(date='20220114')
    print('..')
