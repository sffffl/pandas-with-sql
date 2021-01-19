'''
#获得本地数据库的信息
import pandas as pd
import MySQLdb
mysql_cn = MySQLdb.connect(host='localhost', port=3306,user='root', passwd='fangzhou0628', db='mysql')
df = pd.read_sql('select * from func', con=mysql_cn)
mysql_cn.close()
print(df)      #这样就读取了mysql数据库中的func表
'''


#sql = '''
#select ...;
#select * from employee;
#'''


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

#实现分组后组内排序取2010年以来最新值
#sql_cmd1 = 'select a.S_INFO_WINDCODE,a.S_DQ_ADJFACTOR from (select S_INFO_WINDCODE,S_DQ_ADJFACTOR,row_number() over(partition by S_INFO_WINDCODE order by TRADE_DT desc)  as n from dbo.ASHAREEODPRICES where TRADE_DT>20091231) a where n<=1'
#df1= pd.read_sql(sql=sql_cmd1, con=connect)  #得到每只股最新的复权因子
sql_cmd1 = 'select S_INFO_WINDCODE,S_DQ_ADJFACTOR as US_DQ_ADJFACTOR from dbo.ASHAREEODPRICES where TRADE_DT=20210115 order by S_INFO_WINDCODE'
df1= pd.read_sql(sql=sql_cmd1, con=connect)  #得到每只股最新的复权因子

#将所有的信息存入adj_update表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'nwind' ,'utf8'))
con = engine.connect() #创建连接
df1.to_sql('adj_update', con=con, if_exists='append', index=False)

#获得202010月年以来每只股的相关信息（包括复权因子）
sql_cmd2 = "select S_INFO_WINDCODE,S_DQ_CLOSE,S_DQ_ADJCLOSE,S_DQ_CLOSE*S_DQ_ADJFACTOR as AS_DQ_ADJCLOSE,TRADE_DT,S_DQ_ADJFACTOR from dbo.ASHAREEODPRICES where TRADE_DT>20191231 order by S_INFO_WINDCODE, TRADE_DT"
df2 = pd.read_sql(sql=sql_cmd2, con=connect)

#将两个结果合并成一张表，以第一张表为主表(主要是因为一部分股票退市，所以在合并的时候要以第一张所有在市的股票为主表)
result = pd.merge(df1, df2, how='left',on='S_INFO_WINDCODE')
result['PS_DQ_ADJCLOSE'] = result['S_DQ_ADJCLOSE']/result['US_DQ_ADJFACTOR']

#将所有的信息存入test表
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'nwind' ,'utf8'))
con = engine.connect() #创建连接
result.to_sql('test', con=con, if_exists='append', index=False)

