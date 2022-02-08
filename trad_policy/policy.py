
from model import GetModel

class Policy(object):

    def __init__(self, data):
        self.models = GetModel()
        self.data = data
        self.predicts = self.models.predict(self.data)

    def get_buy_act(self):
        buy_stocks_list = self.data['ts_code'][self.predicts==1]

    def get_sell_act(self):


if __name__ == '__main__':
    from data import DataPrep



