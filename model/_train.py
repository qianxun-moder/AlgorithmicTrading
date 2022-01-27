import pickle

from _featuer_engineer import FeatureEng
from sklearn.model_selection import train_test_split
from sklearn import metrics
import lightgbm as lgb
# import pickle as pkl
import dill



class Model(object):

    def __init__(self):
        data = FeatureEng()
        self.X, self.Y = data.get_train_data()
        self.data = data

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

        self.gbm = gbm
        # pickle.dump(self, open('models/post1_lgbm.md', 'wb'))
        return gbm

    def predict(self):
        data = self.data
        x = data.get_predict_date()

        y_pre = self.gbm.predict(x)


if __name__ == '__main__':
    md = Model()
    md.train()
    dill.dump(md, open('./models/post1_lgbm.md', 'wb'))

    print('..')
