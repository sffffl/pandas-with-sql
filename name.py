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
# sql 命令
sql_cmd = "select distinct(S_INFO_WINDCODE) from dbo.ASHAREEODPRICES where TRADE_DT>20091231 order by S_INFO_WINDCODE "
# sql_cmd = "select S_INFO_WINDCODE as 'wind代码',TRADE_DT as '交易日期',S_DQ_CLOSE as '收盘价',S_DQ_PRECLOSE as '昨收盘价',S_DQ_ADJFACTOR as '复权因子' from dbo.ASHAREEODPRICES where TRADE_DT>20091231 and S_INFO_WINDCODE='000001.SZ' order by TRADE_DT"
df = pd.read_sql(sql=sql_cmd, con=connect)
print(df.iloc[3,])