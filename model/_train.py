
from _my_model1 import Model
from _my_model2 import Model as Model2
import pickle as pkl
import json
from data import MysqlOpt






if __name__ == '__main__':
    if False:
        from data._data_preprocess import DataPrep

        data_pro = DataPrep(start_date='20220111', end_date='20220115')
        md = Model()
        md.train(data_pro.get_data(start_date='20220111', end_date='20220115'))
        pkl.dump(md, open('./models/post1_lgbm.md', 'wb'))
    else:
        db_config_file = '../db_config.json'
        with open(db_config_file) as f:
            db_cfg = json.load(f)
        db = MysqlOpt(db_cfg['url'], db_cfg['port'], db_cfg['user'], db_cfg['password'], 'algtrd_db')
        md = Model2(db=db)
        md.train()
        pkl.dump(md, open('./models/post1_lgbm2.md', 'wb'))
    print('..')
