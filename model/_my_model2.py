
from _data_preprocess2 import DataPrep

from sklearn.model_selection import train_test_split
from sklearn import metrics
import datetime
import lightgbm as lgb


class Model(object):

    def __init__(self, db=None):
        self.db = db
        self.data_pre = DataPrep(db=self.db)

    def get_label(self, data):
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

    def get_train_data(self, start_date=None, end_date=None):
        if (start_date is None) or (end_date is None):
            tmp = datetime.datetime.now()
            end_date = tmp.strftime('%Y%m%d')
            start_date = (tmp-datetime.timedelta(days=30)).strftime('%Y%m%d')
        data = self.data_pre.get_data(start_date=start_date, end_date=end_date)
        data = self.get_label(data)

        cp_col = ['ts_code']
        data_col = ['trade_date']
        y_cols = ['post1', 'post3', 'post5', 'y1', 'y2', 'y3']
        ft_cols = list(set(data.columns.tolist()) - set(cp_col + data_col + y_cols))

        tmp = data[~data['y1'].isna()]
        X = tmp[ft_cols].to_numpy()
        Y = (tmp['y1'] > 1.05).astype('int').to_numpy()

        return X, Y

    def get_predict_data(self):
        date = datetime.date().strftime('%Y%m%d')
        data = self.data_pre.get_data(date=date)

        cp_col = ['ts_code']
        data_col = ['trade_date']
        y_cols = ['post1', 'post3', 'post5', 'y1', 'y2', 'y3']
        ft_cols = list(set(data.columns.tolist()) - set(cp_col + data_col + y_cols))

        X = data[ft_cols].to_numpy()

        return X

    def train(self):
        self.X, self.Y = self.get_train_data()
        x_train, x_test, y_train, y_test = train_test_split(self.X, self.Y, test_size=0.25)
        gbm = lgb.LGBMClassifier()

        gbm.fit(x_train, y_train)

        y_pre = gbm.predict(x_test, num_iteration=gbm.best_iteration_)

        self.acc = metrics.accuracy_score(y_test, y_pre)
        self.precision = metrics.precision_score(y_test, y_pre)
        self.recall = metrics.recall_score(y_test, y_pre)
        self.f1_score = metrics.f1_score(y_test, y_pre)
        self.auc = metrics.roc_auc_score(y_test, y_pre)
        self.total_num = y_test.shape[0]
        self.p_num = y_test.reshape(-1, ).sum()
        self.n_num = self.total_num - self.p_num
        self.pp_num = y_pre.reshape(-1, ).sum()
        self.pn_num = self.total_num - self.pp_num
        self.cf_mt = metrics.confusion_matrix(y_test, y_pre)

        self.model = gbm
        return gbm


    def predict(self, data=None):
        x = self.get_predict_data(data)

        y_pre = self.model.predict(x)
        return y_pre