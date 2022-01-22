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
        self.date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    def retry_opt(func, rt_times=60):

        # @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            sdata = None
            req_success = False
            for i in range(rt_times):
                try:
                    if ('limit' in kwargs.keys()) and (kwargs['limit'] != None):
                        limit = kwargs['limit'] = kwargs['limit']
                        kwargs['offset'] = 0
                        sdata = func(self, *args, **kwargs)
                        offset = limit
                        while sdata.shape[0] > 0:
                            kwargs['offset'] = offset
                            tmp = func(self, *args, **kwargs)
                            if tmp.shape[0] == 0:
                                break
                            else:
                                sdata = sdata.append(tmp)
                                offset += limit
                    else:
                        sdata = func(self, *args, **kwargs)

                    req_success = True
                    break
                except Exception as e:
                    time.sleep(2)
                    print(f"{datetime.datetime.now()} : {func.__name__} retry ts_req {i}'s")
                    continue
            if req_success:
                return sdata
            else:
                raise RuntimeError(f'{datetime.datetime.now()} : {func.__name__} req data error!')

        return wrapper

    @retry_opt
    def stocks_info(self, limit=None, offset=None):

        stocks = self.ts.stock_basic(
            fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')

        return stocks

    @retry_opt
    def trade_cal(self, date=None, excha_list=['SSE', 'SZSE'], start_date=None, end_date=None, limit=None, offset=None):

        if (start_date is None) and (end_date is None):
            if date is None:
                date = self.date
            cal = self.ts.trade_cal(exchange=excha_list[0], cal_date=date, limit=limit, offset=offset)

            for e in excha_list[1:]:
                tcal = self.ts.trade_cal(exchange=e, cal_date=date)
                cal = cal.append(tcal)
        else:
            if start_date is None:
                start_date = self.date
            else:
                end_date = self.date

            cal = self.ts.trade_cal(exchange=excha_list[0], start_date=start_date, end_date=end_date, limit=limit,
                                    offset=offset)

            for e in excha_list[1:]:
                tcal = self.ts.trade_cal(exchange=e, start_date=start_date, end_date=end_date, limit=limit,
                                         offset=offset)
                cal = cal.append(tcal)

        return cal

    @retry_opt
    def daily(self, date=None, limit=None, offset=None):
        '''
        单日日行情
        :return:
        '''
        if date is None:
            date = self.date
        daily_data = self.ts.daily(trade_date=date, limit=limit, offset=offset)

        return daily_data

    def daily_hist(self, start_date=None, end_date=None):
        '''
        历史日行情
        :param start_date:
        :param end_date:
        :return:
        '''

        if (start_date is None) or (end_date is None):
            raise RuntimeError('param error')

        s_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        e_date = datetime.datetime.strptime(end_date, '%Y%m%d')

        diff_days = (e_date - s_date).days

        daily = self.daily(date=start_date)

        for d in range(1, diff_days):
            date = (s_date + datetime.timedelta(days=d)).strftime('%Y%m%d')
            tmp = self.daily(date=date)
            daily = daily.append(tmp)
            if d % 10 == 0:
                print(
                    f'{datetime.datetime.now()} : daily_hist read date {date} success! read {tmp.shape[0]} lines!')

        return daily

    @retry_opt
    def suspend(self, date=None, limit=None, offset=None):
        '''
        单日停复牌信息
        :return:
        '''

        if date is None:
            date = self.date
        sp = self.ts.suspend_d(trade_date=date, limit=limit, offset=offset)
        return sp

    def suspend_his(self, start_date=None, end_date=None):
        '''
        历史停复牌信息
        :return:
        '''
        if (start_date is None) or (end_date is None):
            raise RuntimeError('param error')

        s_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        e_date = datetime.datetime.strptime(end_date, '%Y%m%d')

        diff_days = (e_date - s_date).days
        sus = self.suspend(date=start_date)

        for d in range(1, diff_days):
            date = (s_date + datetime.timedelta(days=d)).strftime('%Y%m%d')
            tmp = self.suspend(date=date)
            sus = sus.append(tmp)
            if d % 10 == 0:
                print(
                    f'{datetime.datetime.now()} : suspend_his read date {date} success! read {tmp.shape[0]} lines!')
        return sus

    @retry_opt
    def moneyflow(self, date=None, limit=None, offset=None):
        '''
        当日个股资金流向
        :return:
        '''
        if date is None:
            date = self.date

        mf = self.ts.moneyflow(trade_date=date,
                               fields=["ts_code", "trade_date", "buy_sm_vol", "buy_sm_amount", "sell_sm_vol",
                                       "sell_sm_amount", "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
                                       "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount", "buy_elg_vol",
                                       "buy_elg_amount", "sell_elg_vol", "sell_elg_amount", "net_mf_vol",
                                       "net_mf_amount", "trade_count"], limit=limit, offset=offset)
        return mf

    def moneyflow_his(self, start_date=None, end_date=None):
        '''
        历史个股资金流向
        :return:
        '''
        if (start_date is None) or (end_date is None):
            raise RuntimeError('param error')

        s_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        e_date = datetime.datetime.strptime(end_date, '%Y%m%d')

        diff_days = (e_date - s_date).days
        mfh = self.moneyflow(date=start_date, limit=4500)

        for d in range(1, diff_days):
            date = (s_date + datetime.timedelta(days=d)).strftime('%Y%m%d')
            tmp = self.moneyflow(date=date, limit=4500)
            mfh = mfh.append(tmp)
            if d % 10 == 0:
                print(f'{datetime.datetime.now()} : moneyflow_his read date {date} success! read {tmp.shape[0]} lines!')

        return mfh

    @retry_opt
    def limit_list(self, date=None, limit=None, offset=None):
        '''
        当日涨跌停统计
        :return:
        '''
        if (limit is None) or (offset is None):
            raise RuntimeError('param error')

        if date is None:
            date = self.date

        lml = self.ts.limit_list(trade_date=date, limit=limit, offset=offset)

        return lml

    def limit_list_hist(self, start_date=None, end_date=None):
        '''
        历史涨跌停统计
        :return:
        '''
        if (start_date is None) or (end_date is None):
            raise RuntimeError('param error')

        s_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        e_date = datetime.datetime.strptime(end_date, '%Y%m%d')

        diff_days = (e_date - s_date).days
        lmlh = self.limit_list(date=start_date, limit=1000)

        for d in range(1, diff_days):
            date = (s_date + datetime.timedelta(days=d)).strftime('%Y%m%d')
            tmp = self.limit_list(date=date, limit=1000)
            lmlh = lmlh.append(tmp)
            if d % 10 == 0:
                print(
                    f'{datetime.datetime.now()} : limit_list_hist read date {date} success! read {tmp.shape[0]} lines!')

        return lmlh


if __name__ == '__main__':
    my_token_file = '../../my_token.txt'
    tsreq = TSReq(my_token_file)

    # sdata = tsreq.stocks_info()
    # cal = tsreq.trade_cal()
    # daily = tsreq.daily(date='20220116')

    daily_his = tsreq.limit_list(date='20220113')
    print('..')
