
import pandas as pd
import json

from sqlalchemy import create_engine

class MysqlOpt(object):

    def __init__(self, db_config_file=None, url=None, port=None, user=None, pw=None, db=None, charset='utf8'):

        if db_config_file is not None:
            with open(db_config_file) as f:
                db_cfg = json.load(f)
            url = db_cfg['url']
            port = db_cfg['port']
            user = db_cfg['user']
            pw = db_cfg['password']

        if url is not None and db is not None:
            engine_url = f'mysql+pymysql://{user}:{pw}@{url}:{port}/{db}?charset={charset}&use_unicode=1'
        else:
            raise ('param error')

        self.mysql = create_engine(engine_url)

    def save(self, table_name=None, data=None, if_exists='fail'):

        if (table_name is None) or (data is None):
            raise RuntimeError('param error')
        if if_exists not in ['fail', 'replace', 'append']:
            raise RuntimeError('param if_exists value must in [“fail”, “replace”, “append”]')

        data.to_sql(table_name, self.mysql, index=False, if_exists=if_exists, chunksize=5000)

    def read(self, read_sql=None):

        if read_sql is None:
            raise RuntimeError('param error')

        data = pd.read_sql_query(read_sql, self.mysql)

        return data

