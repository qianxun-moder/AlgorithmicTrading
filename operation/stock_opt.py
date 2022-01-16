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
        self.db.save('stocks_basinfo_cn', data=tsd, if_exists='replace')


if __name__ == '__main__':
    opt = StockOpt(token_file='../my_token.txt', db_config_file='../db_config.json', db_name='algtrd_db')

    opt.update_stocks()

    print('..')
