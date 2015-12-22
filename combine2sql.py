#encoding:utf-8
import regex as  re
import string
table_name_regexp=re.compile("(?<=create table )\S+")
join_name_regexp =re.compile("(?<=group by )[^;]+")
table_column_regexp=re.compile("(?<=\) as )[^, ]+")

colums=[]
join_colums=[]


table_name=""
join_name=[]
table_column=[]
isAdd=False

skiplinenum=11

f= open("train.sql","r")
while True:
	line = f.readline()
	if not line:
		break
	skiplinenum=skiplinenum-1
	if(skiplinenum>0):
		continue
	if isAdd:
		for i in xrange(0,len(table_column)):
			colums.append(".".join([table_name,table_column[i]]))
		
		tmpstr=" and ".join(("c."+join_name[i]+" = "+table_name+"."+join_name[i]) for i in xrange(0,len(join_name)))
		join_colums.append("left outer join "+table_name+" on(" +tmpstr+" )")

		table_name=""
		join_name=[]
		table_column=[]
		isAdd=False
	else:
		if string.strip(line) =="" and len(table_name)>0:
			isAdd=True
		else:
			if line.find("create")>-1:
				table_name=table_name_regexp.findall(line)[0]
			elif line.find("select")>-1:
				xtmp=join_name_regexp.findall(line)[0].split(",")
				for i in xrange(0,len(xtmp)):
					join_name.append(xtmp[i])
				print xtmp
				ytmp=table_column_regexp.findall(line)
				for j in xrange(0,len(ytmp)):
					table_column.append(ytmp[j])
				#print ytmp
f.close()


for x in colums:
	print x,","
for x in join_colums:
	print x