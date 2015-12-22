hive -e "
set hive.cli.print.header=true; 
set mapreduce.map.memory.mb=4096;
set mapred.child.map.java.opts=-Xmx800M;
set mapreduce.map.java.opts=-Xmx800M;
set mapreduce.reduce.memory.mb=4096;
set mapred.child.reduce.java.opts=-Xmx3800M;
set mapreduce.reduce.java.opts=-Xmx3800M;
set hive.optimize.reducededuplication = false;
use train_final;select distinct * from train_sample_twenty;" > "train_final.csv"
hive -e "
set hive.cli.print.header=true; 
set mapreduce.map.memory.mb=4096;
set mapred.child.map.java.opts=-Xmx800M;
set mapreduce.map.java.opts=-Xmx800M;
set mapreduce.reduce.memory.mb=4096;
set mapred.child.reduce.java.opts=-Xmx3800M;
set mapreduce.reduce.java.opts=-Xmx3800M;
set hive.optimize.reducededuplication = false;
set hive.cli.print.header=true; use test_final;select distinct * from test_sample_twenty;" > "test_final.csv"
hive -e "set hive.cli.print.header=true; 
set mapreduce.map.memory.mb=4096;
set mapred.child.map.java.opts=-Xmx800M;
set mapreduce.map.java.opts=-Xmx800M;
set mapreduce.reduce.memory.mb=4096;
set mapred.child.reduce.java.opts=-Xmx3800M;
set mapreduce.reduce.java.opts=-Xmx3800M;
set hive.optimize.reducededuplication = false;
set hive.cli.print.header=true;use submit_final; select distinct * from submit;" > "submit_final.csv"