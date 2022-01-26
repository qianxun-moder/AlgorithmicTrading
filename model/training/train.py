
import sys

sys.path.append('..')
from feature.data_preprocess import DataPrep
from sklearn.model_selection import train_test_split
from sklearn import metrics
import lightgbm as lgb

class Model(object):

    def __init__(self, X=None, Y=None):
        self.X = X
        self.Y = Y

        self.models = {}

    def train(self):
        x_train, x_test, y_train, y_test = train_test_split(self.X, self.Y, test_size=0.25)
        gbm = lgb.LGBMClassifier()

        gbm.fit(x_train, y_train)

        y_pre = gbm.predict(x_test, num_iteration=gbm.best_iteration_)

        acc = metrics.accuracy_score(y_test, y_pre)
        precision = metrics.precision_score(y_test, y_pre)
        recall = metrics.recall_score(y_test, y_pre)
        f1_score = metrics.f1_score(y_test, y_pre)
        auc = metrics.roc_auc_score(y_test, y_pre)



        print('..')



    def save(self):
        pass

if __name__ == '__main__':
    db_config_file = '../../db_config.json'
    data = DataPrep(db_cfg=db_config_file)
    x, y = data.get_train_data()

    md = Model(x, y)
    md.train()

    print('..')
    print('..')

