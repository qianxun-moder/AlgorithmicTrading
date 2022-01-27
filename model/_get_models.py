import os
import pickle as pkl

class GetModel(object):

    def __init__(self):
        models = {}
        md_files = os.listdir('./models')

        for f in md_files:
            f_path = md_files+'/'+f
            if os.path.isfile(f_path):
                models[f] = pkl.load(open(f_path, 'rb'))
        self.models = models

    def get_models_name(self):
        return list(self.models.keys())

    def predict(self, model_names=None):
        predicts = {}
        if model_names is None:
            model_names = self.models.keys()

        for m in model_names:
            predicts[m] = self.models[m].predict()

