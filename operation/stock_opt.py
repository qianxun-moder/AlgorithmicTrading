import json
import sys

sys.path.append('..')
from data import TSReq
from data import MysqlOpt
from data import DataPrep
from model import GetModel
from profit_status import ProfitStatus
import datetime


class StockOpt(object):

    def __init__(self, token_file=None, db_config_file=None, db_name=None):
        self.ts = TSReq(token_file)
        with open(db_config_file) as f:
            db_cfg = json.load(f)
        self.db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], db_name)
        self.data_trf = DataPrep(db_cfg=db_config_file)
        self.models = GetModel()
        self.profit_st = ProfitStatus(db=self.db)

    def update_stocks(self):
        tsd = self.ts.stocks_info()
        self.db.save('stocks_basinfo_cn', data=tsd, if_exists='replace')
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

    def moneyflow(self, date=None, if_exists='append'):
        tsd = self.ts.moneyflow(date=date)
        self.db.save('moneyflow_cn', data=tsd, if_exists=if_exists)
        return tsd

    def moneyflow_his(self, start_date=None, end_date=None, if_exists='append'):
        tsd = self.ts.moneyflow_his(start_date=start_date, end_date=end_date)
        self.db.save('moneyflow_cn', data=tsd, if_exists=if_exists)
        return tsd

    def limit_list(self, date=None, if_exists='append'):
        tsd = self.ts.limit_list(date=date)
        self.db.save('limit_list_cn', data=tsd, if_exists=if_exists)
        return tsd

    def limit_list_hist(self, start_date=None, end_date=None, if_exists='append'):
        tsd = self.ts.limit_list_hist(start_date=start_date, end_date=end_date)
        self.db.save('limit_list_cn', data=tsd, if_exists=if_exists)
        return tsd

    def data_update(self, date=None):
        '''
        基础数据更新
        :return:
        '''
        self.update_stocks()
        self.daily(date=date)
        self.moneyflow(date=date)
        self.limit_list(date=date)


    def model_train_daily(self, start_date=None, end_date=None):
        '''
        按天周期训练模型
        :return:
        '''
        if (start_date is None) or (end_date is None)
            tmp = datetime.datetime.now()
            start_date = tmp.strftime('%Y%m%d')
            end_date = (tmp-datetime.timedelta(days=30)).strftime('%Y%m%d')

        data = self.data_trf.get_data(start_date=start_date, end_date=end_date)
        self.models.model_update(data=data)


    def model_predict_daily(self, date=None):
        '''
        按天执行模型预测
        :return:
        '''
        data = self.data_trf.get_data(date=date)
        predicts = self.models.predict(data=data)
        return predicts

    def update_profit(self):
        '''
        更新每日收益情况
        :return:
        '''
        self.profit_st.update()

    def get_policy_action(self):
        '''
        运行策略，获取策略动作
        :return:
        '''
        pass

    def stocks_action(self):
        '''
        策略动作执行
        :return:
        '''
        pass

    def back_test(self):
        '''
        回测
        :return:
        '''
        pass


if __name__ == '__main__':
    opt = StockOpt(token_file='../my_token.txt', db_config_file='../db_config.json', db_name='algtrd_db')

    # opt.update_stocks()

    # opt.update_cal(start_date='20120116', end_date='20220115')
    # opt.daily_hist(start_date='20120116', end_date='20220115')
    #
    #
    # opt.suspend_his(start_date='20120116', end_date='20220115')
    # opt.moneyflow_his(start_date='20120116', end_date='20220115')
    # opt.limit_list_hist(start_date='20120116', end_date='20220115')

    tmp = datetime.datetime.now()
    date = tmp.strftime('%Y%m%d')
    train_start_date = (tmp-datetime.timedelta(days=30)).strftime('%Y%m%d')
    opt.data_update(date=date)
    opt.model_train_daily(start_date=train_start_date, end_date=date)
    opt.model_predict_daily()
    opt.model_update_profit()
    opt.get_policy_action()
    opt.stocks_action()
    print('..')


