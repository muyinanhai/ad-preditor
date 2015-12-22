#encoding:utf8
#大概是5万左右行
import pandas as pd
import sys

result=pd.read_csv(sys.argv[1])
result = result[result['target']>0.3]
print len(result)

dicresult=dict()
for i in xrange(0,len(result)):
	print i*1.0/len(result)
	if not result['monitorid'].values[i] in dicresult.keys():
		dicresult[result['monitorid'].values[i]]=set()
	dicresult[result['monitorid'].values[i]].add(result['userid'].values[i])

count=0
f=open(sys.argv[2],'w+')
for key,value in dicresult.items():
    count= count+ len(value)
    print count
    f.write(str(key)+","+(",".join(value))+"\n")
f.close()
print count
print u"记得替换换行等等"
