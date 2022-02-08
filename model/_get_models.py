import os
import pickle as pkl

from data._data_preprocess import DataPrep


# class Model(object):
#     def __init__(self):
#         pass

class GetModel(object):

    def __init__(self):
        models = {}
        models_path = './models'
        md_files = os.listdir(models_path)

        for f in md_files:
            f_path = models_path + '/' + f
            if os.path.isfile(f_path):
                models[f] = pkl.load(open(f_path, 'rb'))
        self.models = models
        self.models_path = models_path

    def get_models_name(self):
        return list(self.models.keys())

    def predict(self, model_names=None, data=None):
        predicts = {}
        if model_names is None:
            model_names = self.models.keys()

        for m in model_names:
            predicts[m] = self.models[m].predict(data=data)

        return predicts

    def model_update(self, model_names=None, data=None):

        if model_names is None:
            model_names = self.models.keys()

        for m in model_names:
            self.models[m].train(data=data)
            pkl.dump(self.models[m], open(self.models_path + '/' + m, 'wb'))


if __name__ == '__main__':
    tmp_data_path = './data2.pkl'
    if os.path.exists(tmp_data_path):
        data = pkl.load(open(tmp_data_path, 'rb'))
    else:
        data_pro = DataPrep()
        data = data_pro.get_data(date='20220113')
        pkl.dump(data, open(tmp_data_path, 'wb'))
    models = GetModel()
    y_pred = models.predict(data=data)

    print('..')
