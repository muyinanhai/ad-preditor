#encoding:utf-8
import pandas as pd
import os
import glob
import re
cnt=dict()
xre=r'-0\.[^\.]+'
p=re.compile(xre)
sumx=0
for f in glob.glob(r'*.csv'):
	score=1
	cnt[f]=score
	sumx+=score
print cnt
print sumx
result=pd.DataFrame()
xtmp=pd.DataFrame()
i=0
for key,val in cnt.items():
	xtmp=pd.read_csv(key)
	if i==0:
		result["target"]=xtmp['target']*val/sumx
		i=i+1
	else:
		result["target"]+=xtmp['target']*val/sumx
result["userid"]=xtmp["userid"]
result["monitorid"]=xtmp["monitorid"]

result=result.reindex(columns=["userid","monitorid","target"])
result.to_csv("weighted_blend.csv",index=False)


