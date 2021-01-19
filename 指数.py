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

#获取当前时间
import datetime
today = datetime.date.today()
today = today.strftime('%Y%m%d')

#得到wind指数
sql_cmd1 = "select S_INFO_WINDCODE,S_INFO_COMPNAME from dbo.AINDEXDESCRIPTION where S_INFO_CODE like '882%' and S_INFO_EXCHMARKET='WIND' order by S_INFO_CODE"
df1 = pd.read_sql(sql=sql_cmd1, con=connect)

#得到wind指数价格
sql_cmd11 = "select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from dbo.AINDEXWINDINDUSTRIESEOD where S_INFO_WINDCODE like '88%' order by S_INFO_WINDCODE,TRADE_DT"
df11 = pd.read_sql(sql=sql_cmd11, con=connect)
df11['TRADE_DT'] = df11['TRADE_DT'].apply(str)
df11 = df11[df11['TRADE_DT'] == today]
result1 = pd.merge(df1, df11, how='left', on='S_INFO_WINDCODE').dropna()

#将所有的信息存入wind_index_price表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'index', 'utf8'))
con = engine.connect() #创建连接
result1.to_sql('wind_index_price', con=con, if_exists='append', index=False)

#得到中信指数
sql_cmd2 = "select S_INFO_WINDCODE,S_INFO_COMPNAME from dbo.AINDEXDESCRIPTION where S_INFO_CODE like 'CI00%' and len(S_INFO_CODE)=8 order by S_INFO_CODE"
df2 = pd.read_sql(sql=sql_cmd2, con=connect)

#得到中信指数价格
sql_cmd22 = "select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from dbo.AINDEXINDUSTRIESEODCITICS order by S_INFO_WINDCODE,TRADE_DT"
df22 = pd.read_sql(sql=sql_cmd22, con=connect)
df22['TRADE_DT'] = df22['TRADE_DT'].apply(str)
df22 = df11[df22['TRADE_DT'] == today]
result2 = pd.merge(df2, df22, how='left', on='S_INFO_WINDCODE').dropna()

#将所有的信息存入zx_index_price表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'index', 'utf8'))
con = engine.connect()
result2.to_sql('zx_index_price', con=con, if_exists='append', index=False)




'''
#得到申万指数
sql_cmd3 = "select S_INFO_WINDCODE from dbo.AINDEXDESCRIPTION where S_INFO_WINDCODE like '%.SI' order by S_INFO_CODE"
df3 = pd.read_sql(sql=sql_cmd3, con=connect)
df3['S_INFO_COMPNAME'] = 'SW'

#得到申万指数价格
sql_cmd33 = "select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from dbo.AINDEXEODPRICES where S_INFO_WINDCODE like '%.SI' order by S_INFO_WINDCODE,TRADE_DT"
df33 = pd.read_sql(sql=sql_cmd33, con=connect)
print(df33)
result3 = pd.merge(df3, df33, how='left',on='S_INFO_WINDCODE').dropna()

#将所有的信息存入all_index表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'index', 'utf8'))
con = engine.connect() #创建连接
result3.to_sql('all_index_price', con=con, if_exists='append', index=False)
'''