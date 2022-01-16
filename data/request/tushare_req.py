import functools
import time

import tushare as ts
import pandas as pd
import datetime


class TSReq(object):

    def __init__(self, token_file=None):
        token = open(token_file, 'rt').read()
        ts.set_token(token)
        self.ts = ts.pro_api()
        self.date = datetime.date

    def retry_opt(func, rt_times=60):

        # @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            sdata = None
            req_success = False
            for i in range(rt_times):
                try:
                    sdata = func(self, *args, **kwargs)
                    req_success = True
                    break
                except Exception as e:
                    time.sleep(1)
                    continue
            if req_success:
                return sdata
            else:
                raise RuntimeError(f'{datetime.datetime.now()} : {func.__name__} req data error!')
        return wrapper

    @retry_opt
    def stocks_info(self):

        stocks = self.ts.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')

        return stocks

    @retry_opt
    def trade_cal(self, date=None, excha_list=['SSE', 'SZSE'], start_date=None, end_date=None):

        if (start_date is None) and (end_date is None):
            if date is None:
                date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y%m%d')
            cal = self.ts.trade_cal(exchange=excha_list[0], cal_date=date)

            for e in excha_list[1:]:
                tcal = self.ts.trade_cal(exchange=e, cal_date=date)
                cal = cal.append(tcal)
        else:
            if start_date is None:
                start_date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y%m%d')
            else:
                end_date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y%m%d')

            cal = self.ts.trade_cal(exchange=excha_list[0], start_date=start_date, end_date=end_date)

            for e in excha_list[1:]:
                tcal = self.ts.trade_cal(exchange=e, start_date=start_date, end_date=end_date)
                cal = cal.append(tcal)

        return cal
+

    @retry_opt
    def daily(self, date=None):
        '''
        单日日行情
        :return:
        '''

        if date is None:

            daily_data = self.ts.daily()

    @retry_opt
    def daily_hist(self, start_date=None, end_date=None):
        '''
        历史日行情
        :param start_date:
        :param end_date:
        :return:
        '''
        pass

    @retry_opt
    def suspend(self):
        '''
        当日停复牌信息
        :return:
        '''
        pass

    @retry_opt
    def suspend_his(self):
        '''
        历史停复牌信息
        :return:
        '''
        pass

    @retry_opt
    def moneyflow(self):
        '''
        当日个股资金流向
        :return:
        '''
        pass

    @retry_opt
    def moneyflow_his(self):
        '''
        历史个股资金流向
        :return:
        '''
        pass

    @retry_opt
    def limit_list(self):
        '''
        当日涨跌停统计
        :return:
        '''
        pass

    @retry_opt
    def limit_list_hist(self):
        '''
        历史涨跌停统计
        :return:
        '''
        pass

if __name__ == '__main__':
    my_token_file = '../../my_token.txt'
    tsreq = TSReq(my_token_file)

    # sdata = tsreq.stocks_info()
    cal = tsreq.trade_cal()

    print('..')