

from datetime import datetime

import pandas as pd




# def merge_data(self, date = None, start_date=None, end_date=None):
#     data = self.data
#     stock_data = data.stocks()
#     dl = data.daily(date=date, start_date=start_date, end_date=end_date)
#     mf = data.moneyflow(date=date, start_date=start_date, end_date=end_date)
#     ls = data.limit_list(date=date, start_date=start_date, end_date=end_date)
#
#     mgd = pd.merge(left=dl, right=stock_data, left_on='ts_code', right_on='ts_code', how='left')
#     mgd = pd.merge(left=mgd, right=mf, left_on=['ts_code', 'trade_date'], right_on=['ts_code', 'trade_date'],
#                    how='left')
#     mgd = pd.merge(left=mgd, right=ls, left_on=['ts_code', 'trade_date'], right_on=['ts_code', 'trade_date'],
#                    how='left')
#
#     return mgd


def get_label(data):
    data['post1'] = data.sort_values('trade_date').groupby('ts_code')['daily-close-0'].shift(1)
    data['post3'] = data.sort_values('trade_date').groupby('ts_code')['daily-close-0'].shift(3)
    data['post5'] = data.sort_values('trade_date').groupby('ts_code')['daily-close-0'].shift(5)
    data['y1'] = data['post1'] / data['daily-close-0']
    data['y2'] = data['post3'] / data['daily-close-0']
    data['y3'] = data['post5'] / data['daily-close-0']

    cp_col = ['ts_code']
    data_col = ['trade_date']
    y_cols = ['post1', 'post3', 'post5', 'y1', 'y2', 'y3']
    ft_cols = list(set(data.columns.tolist()) - set(cp_col + data_col + y_cols))

    return data

def get_train_data(data):
    # data = self.merge_data(start_date='20220111', end_date='20220115')
    data = get_label(data)

    cp_col = ['ts_code']
    data_col = ['trade_date']
    y_cols = ['post1', 'post3', 'post5', 'y1', 'y2', 'y3']
    ft_cols = list(set(data.columns.tolist()) - set(cp_col + data_col + y_cols))

    tmp = data[~data['y1'].isna()]
    X = tmp[ft_cols].to_numpy()
    Y = (tmp['y1'] > 1.05).astype('int').to_numpy()

    return X, Y

def get_predict_date(data):

    cp_col = ['ts_code']
    data_col = ['trade_date']
    y_cols = ['post1', 'post3', 'post5', 'y1', 'y2', 'y3']
    ft_cols = list(set(data.columns.tolist()) - set(cp_col + data_col + y_cols))

    X = data[ft_cols].to_numpy()

    return X
