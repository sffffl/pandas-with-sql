# 导入必要模块
import pandas as pd
from sqlalchemy import create_engine

# 初始化数据库连接，使用pymysql模块
engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}".format('root', 'fangzhou0628', 'localhost:3306', 'sys' ,'utf8'))

# 读取本地CSV文件
df = pd.read_csv("C:/Users/fz/Desktop/奖学金.csv", sep=',')

# 将新建的DataFrame储存为MySQL中的数据表，不储存index列
df.to_sql('mpg', engine, index=False, if_exists="replace")
print("Write to MySQL successfully!")