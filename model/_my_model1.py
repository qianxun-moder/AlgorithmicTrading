from _featuer_engineer import get_train_data, get_predict_date
from sklearn.model_selection import train_test_split
from sklearn import metrics
import lightgbm as lgb


class Model(object):

    def __init__(self):
        pass

    def train(self, data):
        self.X, self.Y = get_train_data(data)
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
        x = get_predict_date(data)

        y_pre = self.model.predict(x)
        return y_pre