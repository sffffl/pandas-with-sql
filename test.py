import numpy as np
import pandas as pd
data1 = {'four':np.arange(4,9), 'one':range(1,6), 'three':[2,2,3,3,4],'two':range(2,7)}
df=pd.DataFrame(data1,index=list('aaabb'))

print(df)
result=None
for name, group in df.groupby('three'):   #分类计算前复权价格
    a=group.iloc[-1]['two']
    group['PS_DQ_ADJCLOSE']=group['four']/a
    result = pd.concat([result, group['PS_DQ_ADJCLOSE']], axis=0)
print(result)
df['PS_DQ_ADJCLOSE'] = result
print(df)