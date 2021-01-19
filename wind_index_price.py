# 获得远程数据库的信息
import pandas as pd
import pymssql

# 用sqlalchemy构建数据库链接engine
db_info = {'user': 'hcquant',
           'password': 'Quant_17885',
           'host': 'nas.hcyjs.com',
           'port': 19433,
           'database': 'nWind',  # 这里我们事先指定了数据库，后续操作只需要表即可
           'charset': 'utf8'
           }
connect = pymssql.connect(**db_info)

#得到wind指数
sql_cmd1 = "select S_INFO_WINDCODE from dbo.AINDEXDESCRIPTION where S_INFO_CODE like '882%' and S_INFO_EXCHMARKET='WIND' order by S_INFO_CODE"
df1 = pd.read_sql(sql=sql_cmd1, con=connect)

#得到wind指数价格
sql_cmd11 = "select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from dbo.AINDEXWINDINDUSTRIESEOD where S_INFO_WINDCODE like '88%' order by S_INFO_WINDCODE,TRADE_DT"
df11 = pd.read_sql(sql=sql_cmd11, con=connect)
result1 = pd.merge(df1, df11, how='left', on='S_INFO_WINDCODE').dropna()

#将所有的信息存入all_index表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'index', 'utf8'))
con = engine.connect() #创建连接
result1.to_sql('wind_index_price', con=con, if_exists='append', index=False)
