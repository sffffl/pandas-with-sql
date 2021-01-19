# 获得远程数据库的信息
import pandas as pd
import pymssql

'''
# 用sqlalchemy构建数据库链接engine
db_info = {'user': 'hcquant',
           'password': 'Quant_17885',
           'host': 'nas.hcyjs.com',
           'port': 19433,
           'database': 'nWind',  # 这里我们事先指定了数据库，后续操作只需要表即可
           'charset': 'utf8'
           }
connect = pymssql.connect(**db_info)

# sql 命令
sql_cmd = "select S_INFO_WINDCODE,S_DQ_CLOSE,S_DQ_ADJCLOSE,S_DQ_CLOSE*S_DQ_ADJFACTOR as AS_DQ_ADJCLOSE,TRADE_DT,S_DQ_ADJFACTOR from dbo.ASHAREEODPRICES where TRADE_DT>20091231 and S_INFO_WINDCODE='000001.SZ'"
df = pd.read_sql(sql=sql_cmd, con=connect)

#将获得的数据导入本地数据库
#建立连接
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'nwind' ,'utf8'))
con = engine.connect() #创建连接
df.to_sql('test', con=con, if_exists='replace', index=False)
'''


#获取当前日期
import datetime
today=datetime.date.today()
formatted_today=today.strftime('%Y%m%d')
print(formatted_today)


# 用sqlalchemy构建数据库链接engine
db_info = {'user': 'hcquant',
           'password': 'Quant_17885',
           'host': 'nas.hcyjs.com',
           'port': 19433,
           'database': 'nWind',  # 这里我们事先指定了数据库，后续操作只需要表即可
           'charset': 'utf8'
           }
connect = pymssql.connect(**db_info)

df1 = pd.read_sql(sql="select S_INFO_WINDCODE,S_DQ_ADJFACTOR from dbo.ASHAREEODPRICES where TRADE_DT = 20210114 and S_INFO_WINDCODE='000001.SZ' ", con=connect)  #这是每天更新的复权因子

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'nwind' ,'utf8'))
con = engine.connect() #创建连接
df1.to_sql('adj_update', con=con, if_exists='replace', index=False)
