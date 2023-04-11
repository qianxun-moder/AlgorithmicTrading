from data import TSReq
from data import MysqlOpt

my_token_file = '../my_token.txt'
ts = TSReq(my_token_file)

db_config_file = '../db_config.json'
db_name='algtrd_db'
db = MysqlOpt(db_config_file, db=db_name)

stocks_info = ts.stocks_info()
table_name = 'stocks_basinfo_cn'
db.save(table_name, stocks_info, if_exists='replace')

print('..')