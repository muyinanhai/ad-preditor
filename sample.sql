use train_final;
drop table if exists train_sample_ten;
create table train_sample_ten as
select * from(ｄ
select * from(
select distinct * from train a  where (target=0 or target is null) distribute by rand() sort by rand() limit 124210
)c
union all
select distinct * from train b where target>0 
)xyz;


use test_final;
drop table if exists test_sample_ten;
create table test_sample_ten as
select * from(
select * from(
select distinct * from test a  where (target=0 or target is null) distribute by rand() sort by rand() limit 124210
)c
union all
select distinct * from test b where target>0 
)xyz;

