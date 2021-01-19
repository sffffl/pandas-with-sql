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

#得到中信指数
sql_cmd2 = "select S_INFO_WINDCODE from dbo.AINDEXDESCRIPTION where S_INFO_CODE like 'CI00%' and len(S_INFO_CODE)=8 order by S_INFO_CODE"
df2 = pd.read_sql(sql=sql_cmd2, con=connect)

#得到中信指数价格
sql_cmd22 = "select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from dbo.AINDEXINDUSTRIESEODCITICS order by S_INFO_WINDCODE,TRADE_DT"
df22 = pd.read_sql(sql=sql_cmd22, con=connect)
result2 = pd.merge(df2, df22, how='left', on='S_INFO_WINDCODE').dropna()

#将所有的信息存入all_index表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'index', 'utf8'))
con = engine.connect()
result2.to_sql('zx_index_price', con=con, if_exists='append', index=False)