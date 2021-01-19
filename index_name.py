# 获得远程数据库的信息
import pandas as pd
import pymssql

# 用sqlalchemy构建数据库链接engine
db_info = {'user': 'hcquant',
           'password': 'Quant_17885',
           'host': 'nas.hcyjs.com',
           'port': 19433,
           'database': 'nWind',  # 这里我们事先指定了数据库，后续操作只需要表即可
           'charset': 'GB2312'
           }
connect = pymssql.connect(**db_info)


#得到wind指数
sql_cmd1 = "select S_INFO_WINDCODE,S_INFO_COMPNAME from dbo.AINDEXDESCRIPTION where S_INFO_CODE like '882%' and S_INFO_EXCHMARKET='WIND' order by S_INFO_CODE"
df1 = pd.read_sql(sql=sql_cmd1, con=connect)

#得到中信指数
sql_cmd2 = "select S_INFO_WINDCODE,S_INFO_COMPNAME from dbo.AINDEXDESCRIPTION where S_INFO_CODE like 'CI00%' and len(S_INFO_CODE)=8 order by S_INFO_CODE"
df2 = pd.read_sql(sql=sql_cmd2, con=connect)

result = pd.concat([df1, df2], axis=0)

#将所有的信息存入all_index表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'index', 'utf8'))
con = engine.connect()
result.to_sql('index_name', con=con, if_exists='append', index=False)