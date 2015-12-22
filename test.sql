set mapreduce.map.memory.mb=4096;
set mapred.child.map.java.opts=-Xmx800M;
set mapreduce.map.java.opts=-Xmx800M; set mapreduce.reduce.memory.mb=4096;
set mapred.child.reduce.java.opts=-Xmx3800M;
set mapreduce.reduce.java.opts=-Xmx3800M;
set hive.optimize.reducededuplication = false;
set mapred.reduce.tasks=270;
create database test_second;
use test_second;

--user
drop table if exists u_isStableNum;
create table u_isStableNum as
select userId,count(if(isStable=1,1,null)) as u_isStableNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId;

drop table if exists u_isStableClickNum;
create table u_isStableNum as
select userId,count(if(isStable=1,1,null)) as u_isStableNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId;

drop table if exists u_isStableNotClickNum;
create table u_isStableNotClickNum as
select userId,count(if(isStable=1,1,null)) as u_isStableNotClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by userId;

drop table if exists u_allNum;
create table u_allNum as
select userId,count(1) as u_allNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId;

drop table if exists u_clickNum;
create table u_clickNum as
select userId,count(1) as u_clickNum,count(distinct adId) as u_clickAdIdNum,count(distinct monitorId) as u_clickMonitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId;

drop table if exists u_notClickNum;
create table u_notClickNum as
select userId,count(1) as u_notClickNum,count(distinct adId) as u_notClickAdNum,count(distinct monitorId) as u_notClickMonitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by userId;


drop table if exists u_ipAllNum;
create table u_ipAllNum as
select userId,count(distinct IPAdrr) as u_ipAllNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId;

drop table if exists u_ipClickNum;
create table u_ipClickNum as
select userId,count(distinct IPAdrr) as u_ipClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId;

drop table if exists u_languageNum;
create table u_languageNum as
select userId,count(distinct language) as u_languageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId;

drop table if exists u_languageClickNum;
create table u_languageClickNum as
select userId,count(distinct language) as u_languageClickNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 and isClick=1 group by userId;

drop table if exists u_sysAllNum;
create table u_sysAllNum as
select userId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as u_sysAllNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 group by userId;

drop table if exists u_sysClickNum;
create table u_sysClickNum as
select userId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as u_sysClickNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 and isClick=1 group by userId;


drop table if exists u_sysExplorNum;
create table u_sysExplorNum as
select userId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as u_sysExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId;

drop table if exists u_sysCilckExplorNum;
create table u_sysCilckExplorNum as
select userId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as u_sysCilckExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId;

drop table if exists u_dateAllNum;
create table u_dateAllNum as
select userId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as u_dateAllNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId;

drop table if exists u_dateClickNum;
create table u_dateClickNum as
select userId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as u_dateClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId;


--monitorId

drop table if exists m_allNum;
create table m_allNum as
select monitorId,count(1) as m_allNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_clickUserNum;
create table m_clickUserNum as
select monitorId,count(1) as m_clickNum,count(distinct userId) as m_clickUserNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_notClickUserNum;
create table m_notClickUserNum as
select monitorId,count(1) as m_notClickNum,count(distinct userId) as m_notClickUserNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_languageNum;
create table m_languageNum as
select monitorId,count(distinct language) as m_languageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_clickLanguageNum;
create table m_clickLanguageNum as
select monitorId,count(distinct language) as m_clickLanguageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_notClickLanguageNum;
create table m_notClickLanguageNum as
select monitorId,count(distinct language) as m_notClickLanguageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_monitorNum;
create table m_monitorNum as
select monitorId,count(distinct monitorID) as m_monitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_clickMonitorNum;
create table m_clickMonitorNum as
select monitorId,count(distinct monitorID) as m_clickMonitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_notClickMonitorNum;
create table m_notClickMonitorNum as
select monitorId,count(distinct monitorID) as m_notClickMonitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_IPNum;
create table m_IPNum as
select monitorId,count(distinct IPAdrr) as m_IPNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_clickIPNum;
create table m_clickIPNum as
select monitorId,count(distinct IPAdrr) as m_clickIPNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_notClickIPNum;
create table m_notClickIPNum as
select monitorId,count(distinct IPAdrr) as m_notClickIPNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_visDateNum;
create table m_visDateNum as
select monitorId,count(distinct visDate) as m_visDateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_clickVisDateNum;
create table m_clickVisDateNum as
select monitorId,count(distinct visDate) as m_clickVisDateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_notClickVisDateNum;
create table m_notClickVisDateNum as
select monitorId,count(distinct visDate) as m_notClickVisDateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_sysNum;
create table m_sysNum as
select monitorId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as m_sysNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_sysClickNum;
create table m_sysClickNum as
select monitorId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as m_sysClickNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_sysNotClickNum;
create table m_sysNotClickNum as
select monitorId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as m_sysNotClickNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_sysExplorNum;
create table m_sysExplorNum as
select monitorId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as m_sysExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_sysCilckExplorNum;
create table m_sysCilckExplorNum as
select monitorId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as m_sysCilckExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_sysNotCilckExplorNum;
create table m_sysNotCilckExplorNum as
select monitorId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as m_sysNotCilckExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

drop table if exists m_dateNum;
create table m_dateNum as
select monitorId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as m_dateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by monitorId;

drop table if exists m_dateClickNum;
create table m_dateClickNum as
select monitorId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as m_dateClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by monitorId;

drop table if exists m_dateNotClickNum;
create table m_dateNotClickNum as
select monitorId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as m_dateNotClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by monitorId;

--ad
drop table if exists a_allNum;
create table a_allNum as
select adId,count(1) as a_allNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_clickNum;
create table a_clickNum as
select adId,count(1) as a_clickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_notClickNum;
create table a_notClickNum as
select adId,count(1) as a_notClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;

drop table if exists a_userNum;
create table a_userNum as
select adId,count(distinct userId) as a_userNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_clickUserNum;
create table a_clickUserNum as
select adId,count(distinct userId) as a_clickUserNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_notClickUserNum;
create table a_notClickUserNum as
select adId,count(distinct userId) as a_notClickUserNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;

drop table if exists a_languageNum;
create table a_languageNum as
select adId,count(distinct language) as a_languageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_clickLanguageNum;
create table a_clickLanguageNum as
select adId,count(distinct language) as a_clickLanguageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_notClickLanguageNum;
create table a_notClickLanguageNum as
select adId,count(distinct language) as a_notClickLanguageNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;

drop table if exists a_monitorNum;
create table a_monitorNum as
select adId,count(distinct monitorID) as a_monitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_clickMonitorNum;
create table a_clickMonitorNum as
select adId,count(distinct monitorID) as a_clickMonitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_notClickMonitorNum;
create table a_notClickMonitorNum as
select adId,count(distinct monitorID) as a_notClickMonitorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;


drop table if exists a_IPNum;
create table a_IPNum as
select adId,count(distinct IPAdrr) as a_IPNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_clickIPNum;
create table a_clickIPNum as
select adId,count(distinct IPAdrr) as a_clickIPNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_notClickIPNum;
create table a_notClickIPNum as
select adId,count(distinct IPAdrr) as a_notClickIPNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;


drop table if exists a_visDateNum;
create table a_visDateNum as
select adId,count(distinct visDate) as a_visDateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_clickVisDateNum;
create table a_clickVisDateNum as
select adId,count(distinct visDate) as a_clickVisDateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_notClickVisDateNum;
create table a_notClickVisDateNum as
select adId,count(distinct visDate) as a_notClickVisDateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;


drop table if exists a_sysNum;
create table a_sysNum as
select adId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as a_sysNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 group by adId;

drop table if exists a_sysClickNum;
create table a_sysClickNum as
select adId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as a_sysClickNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_sysNotClickNum;
create table a_sysNotClickNum as
select adId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[0],"")) as a_sysNotClickNum from ynh.sample_train where visdate > 1420732800  and visdate<1434297600 and isClick=0 group by adId;

drop table if exists a_sysExplorNum;
create table a_sysExplorNum as
select adId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as a_sysExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_sysCilckExplorNum;
create table a_sysCilckExplorNum as
select adId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as a_sysCilckExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_sysNotCilckExplorNum;
create table a_sysNotCilckExplorNum as
select adId,count(distinct if(length(mixfeature)>0,split(mixfeature,'_')[1],"")) as a_sysNotCilckExplorNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;

drop table if exists a_dateNum;
create table a_dateNum as
select adId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as a_dateNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by adId;

drop table if exists a_dateClickNum;
create table a_dateClickNum as
select adId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as a_dateClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by adId;

drop table if exists a_dateNotClickNum;
create table a_dateNotClickNum as
select adId,count(distinct from_unixtime(cast(visdate as bigint),'yyyyMMdd')) as a_dateNotClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by adId;

--mu
drop table if exists mu_allNum;
create table m_usermoni_Num as
select userId,monitorId,count(*) as mu_allNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId,monitorId;

drop table if exists mu_clickNum;
create table mu_clickNum as
select userId,monitorId,count(*) as mu_clickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId,monitorId;

drop table if exists mu_notClickNum;
create table mu_notClickNum as
select userId,monitorId,count(*) as mu_notClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by userId,monitorId;

--ua
drop table if exists ua_allNum ;
create table ua_allNum  as
select userId,adId,count(*) as ua_allNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 group by userId,adId;

drop table if exists ua_clickNum;
create table ua_clickNum as
select userID,adID,count(*) as ua_clickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=1 group by userId,adId;

drop table if exists ua_notClickNum;
create table ua_notClickNum as
select userID,adID,count(*) as ua_notClickNum from ynh.sample_train where visdate>1420732800  and visdate<1434297600 and isClick=0 group by userId,adId;

drop table if exists target;
create table target as
select userId,monitorId,sum(isClick) as target from ynh.sample_train where visDate>1434297600 and visDate<1434988800 group by userId,monitorId;

