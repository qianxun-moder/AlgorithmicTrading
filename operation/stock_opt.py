import json
import sys

sys.path.append('..')
from data.request.tushare_req import TSReq
from data.db.mysql import MysqlOpt


class StockOpt(object):

    def __init__(self, token_file=None, db_config_file=None, db_name=None):
        self.ts = TSReq(token_file)
        with open(db_config_file) as f:
            db_cfg = json.load(f)
        self.db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], db_name)

    def update_stocks(self):
        tsd = self.ts.stocks_info()
        self.db.save('stocks_basinfo_cn', data=tsd, if_exists='append')
        return tsd

    def update_cal(self, start_date=None, end_date=None):
        tsd = self.ts.trade_cal(start_date=start_date, end_date=end_date)
        self.db.save('trade_cal_cn', data=tsd, if_exists='append')
        return tsd

    def daily(self, date=None):
        tsd = self.ts.daily(date=date)
        self.db.save('daily_cn', data=tsd, if_exists='append')
        return tsd

    def daily_hist(self, start_date=None, end_date=None, if_exists='append'):
        tsd = self.ts.daily_hist(start_date=start_date, end_date=end_date)
        self.db.save('daily_cn', data=tsd, if_exists=if_exists)
        return tsd

    def suspend(self, date=None, if_exists='append'):
        tsd = self.ts.suspend(date=date)
        self.db.save('suspend_cn', data=tsd, if_exists=if_exists)
        return tsd

    def suspend_his(self, start_date=None, end_date=None, if_exists='append'):
        tsd = self.ts.suspend_his(start_date=start_date, end_date=end_date)
        self.db.save('suspend_cn', data=tsd, if_exists=if_exists)
        return tsd

if __name__ == '__main__':
    opt = StockOpt(token_file='../my_token.txt', db_config_file='../db_config.json', db_name='algtrd_db')

    # opt.update_stocks()

    # opt.update_cal(start_date='20120116', end_date='20220115')
    # opt.daily_hist(start_date='20120116', end_date='20220115')
    opt.suspend_his(start_date='20120116', end_date='20220115')
    print('..')

