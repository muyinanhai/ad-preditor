#encoding:utf-8
import pandas as pd
import os
import glob
import re
cnt=dict()
for f in glob.glob(r'*.csv'):
    cnt[f]=1
print cnt
print len(cnt)
result=pd.DataFrame()
xtmp=pd.DataFrame()
names=None
i=0
for key,val in cnt.items():
    xtmp=pd.read_csv(key)
    names=list(xtmp.columns)
    print key
    if i==0:
        i=i+1
        result['target']=xtmp['target']
    else:
        result['target']+=xtmp['target']

xtmp['target']=result['target']/len(cnt)
xtmp.to_csv(u"blending.csv",index=False)


