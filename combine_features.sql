use submit_final;
set mapreduce.map.memory.mb=4096;
set mapred.child.map.java.opts=-Xmx800M;
set mapreduce.map.java.opts=-Xmx800M;
set mapreduce.reduce.memory.mb=4096;
set mapred.child.reduce.java.opts=-Xmx3800M;
set mapreduce.reduce.java.opts=-Xmx3800M;
set hive.optimize.reducededuplication = false;
create table submit_new_group as
select 
c.userid,
c.adid,
c.monitorid,
u_isStableNum.u_isStableNum ,
u_isStableClickNum.u_isStableClickNum ,
u_isStableNotClickNum.u_isStableNotClickNum ,
u_allNum.u_allNum ,
u_clickNum.u_clickNum ,
u_clickNum.u_clickAdIdNum ,
u_clickNum.u_clickMonitorNum ,
u_notClickNum.u_notClickNum ,
u_notClickNum.u_notClickAdNum ,
u_notClickNum.u_notClickMonitorNum ,
u_ipAllNum.u_ipAllNum ,
u_ipClickNum.u_ipClickNum ,
u_languageNum.u_languageNum ,
u_languageClickNum.u_languageClickNum ,
u_sysAllNum.u_sysAllNum ,
u_sysClickNum.u_sysClickNum ,
u_sysExplorNum.u_sysExplorNum ,
u_sysCilckExplorNum.u_sysCilckExplorNum ,
u_dateAllNum.u_dateAllNum ,
u_dateClickNum.u_dateClickNum ,
m_allNum.m_allNum ,
m_clickUserNum.m_clickNum ,
m_clickUserNum.m_clickUserNum ,
m_notClickUserNum.m_notClickNum ,
m_notClickUserNum.m_notClickUserNum ,
m_languageNum.m_languageNum ,
m_clickLanguageNum.m_clickLanguageNum ,
m_notClickLanguageNum.m_notClickLanguageNum ,
m_monitorNum.m_monitorNum ,
m_clickMonitorNum.m_clickMonitorNum ,
m_notClickMonitorNum.m_notClickMonitorNum ,
m_IPNum.m_IPNum ,
m_clickIPNum.m_clickIPNum ,
m_notClickIPNum.m_notClickIPNum ,
m_visDateNum.m_visDateNum ,
m_clickVisDateNum.m_clickVisDateNum ,
m_notClickVisDateNum.m_notClickVisDateNum ,
m_sysNum.m_sysNum ,
m_sysClickNum.m_sysClickNum ,
m_sysNotClickNum.m_sysNotClickNum ,
m_sysExplorNum.m_sysExplorNum ,
m_sysCilckExplorNum.m_sysCilckExplorNum ,
m_sysNotCilckExplorNum.m_sysNotCilckExplorNum ,
m_dateNum.m_dateNum ,
m_dateClickNum.m_dateClickNum ,
m_dateNotClickNum.m_dateNotClickNum ,
a_allNum.a_allNum ,
a_clickNum.a_clickNum ,
a_notClickNum.a_notClickNum ,
a_userNum.a_userNum ,
a_clickUserNum.a_clickUserNum ,
a_notClickUserNum.a_notClickUserNum ,
a_languageNum.a_languageNum ,
a_clickLanguageNum.a_clickLanguageNum ,
a_notClickLanguageNum.a_notClickLanguageNum ,
a_monitorNum.a_monitorNum ,
a_clickMonitorNum.a_clickMonitorNum ,
a_notClickMonitorNum.a_notClickMonitorNum ,
a_IPNum.a_IPNum ,
a_clickIPNum.a_clickIPNum ,
a_notClickIPNum.a_notClickIPNum ,
a_visDateNum.a_visDateNum ,
a_clickVisDateNum.a_clickVisDateNum ,
a_notClickVisDateNum.a_notClickVisDateNum ,
a_sysNum.a_sysNum ,
a_sysClickNum.a_sysClickNum ,
a_sysNotClickNum.a_sysNotClickNum ,
a_sysExplorNum.a_sysExplorNum ,
a_sysCilckExplorNum.a_sysCilckExplorNum ,
a_sysNotCilckExplorNum.a_sysNotCilckExplorNum ,
a_dateNum.a_dateNum ,
a_dateClickNum.a_dateClickNum ,
a_dateNotClickNum.a_dateNotClickNum ,
m_usermoni_Num.mu_allNum ,
mu_clickNum.mu_clickNum ,
mu_notClickNum.mu_notClickNum ,
ua_allNum.ua_allNum ,
ua_clickNum.ua_clickNum ,
ua_notClickNum.ua_notClickNum
--,target.target
from(
select userid,adid,monitorid from ynh.adtrain 
--train
--where visdate>1420041600 and visdate<1433606400
--test
--where visdate>1420732800 and visdate<1434297600
--submit
where visdate>1421424000 and visdate<1434988800
group by userid,adid,monitorid
)c
left outer join u_isStableNum on(c.userId = u_isStableNum.userId )
left outer join u_isStableClickNum on(c.userId = u_isStableClickNum.userId )
left outer join u_isStableNotClickNum on(c.userId = u_isStableNotClickNum.userId )
left outer join u_allNum on(c.userId = u_allNum.userId )
left outer join u_clickNum on(c.userId = u_clickNum.userId )
left outer join u_notClickNum on(c.userId = u_notClickNum.userId )
left outer join u_ipAllNum on(c.userId = u_ipAllNum.userId )
left outer join u_ipClickNum on(c.userId = u_ipClickNum.userId )
left outer join u_languageNum on(c.userId = u_languageNum.userId )
left outer join u_languageClickNum on(c.userId = u_languageClickNum.userId )
left outer join u_sysAllNum on(c.userId = u_sysAllNum.userId )
left outer join u_sysClickNum on(c.userId = u_sysClickNum.userId )
left outer join u_sysExplorNum on(c.userId = u_sysExplorNum.userId )
left outer join u_sysCilckExplorNum on(c.userId = u_sysCilckExplorNum.userId )
left outer join u_dateAllNum on(c.userId = u_dateAllNum.userId )
left outer join u_dateClickNum on(c.userId = u_dateClickNum.userId )
left outer join m_allNum on(c.monitorId = m_allNum.monitorId )
left outer join m_clickUserNum on(c.monitorId = m_clickUserNum.monitorId )
left outer join m_notClickUserNum on(c.monitorId = m_notClickUserNum.monitorId )
left outer join m_languageNum on(c.monitorId = m_languageNum.monitorId )
left outer join m_clickLanguageNum on(c.monitorId = m_clickLanguageNum.monitorId )
left outer join m_notClickLanguageNum on(c.monitorId = m_notClickLanguageNum.monitorId )
left outer join m_monitorNum on(c.monitorId = m_monitorNum.monitorId )
left outer join m_clickMonitorNum on(c.monitorId = m_clickMonitorNum.monitorId )
left outer join m_notClickMonitorNum on(c.monitorId = m_notClickMonitorNum.monitorId )
left outer join m_IPNum on(c.monitorId = m_IPNum.monitorId )
left outer join m_clickIPNum on(c.monitorId = m_clickIPNum.monitorId )
left outer join m_notClickIPNum on(c.monitorId = m_notClickIPNum.monitorId )
left outer join m_visDateNum on(c.monitorId = m_visDateNum.monitorId )
left outer join m_clickVisDateNum on(c.monitorId = m_clickVisDateNum.monitorId )
left outer join m_notClickVisDateNum on(c.monitorId = m_notClickVisDateNum.monitorId )
left outer join m_sysNum on(c.monitorId = m_sysNum.monitorId )
left outer join m_sysClickNum on(c.monitorId = m_sysClickNum.monitorId )
left outer join m_sysNotClickNum on(c.monitorId = m_sysNotClickNum.monitorId )
left outer join m_sysExplorNum on(c.monitorId = m_sysExplorNum.monitorId )
left outer join m_sysCilckExplorNum on(c.monitorId = m_sysCilckExplorNum.monitorId )
left outer join m_sysNotCilckExplorNum on(c.monitorId = m_sysNotCilckExplorNum.monitorId )
left outer join m_dateNum on(c.monitorId = m_dateNum.monitorId )
left outer join m_dateClickNum on(c.monitorId = m_dateClickNum.monitorId )
left outer join m_dateNotClickNum on(c.monitorId = m_dateNotClickNum.monitorId )
left outer join a_allNum on(c.adId = a_allNum.adId )
left outer join a_clickNum on(c.adId = a_clickNum.adId )
left outer join a_notClickNum on(c.adId = a_notClickNum.adId )
left outer join a_userNum on(c.adId = a_userNum.adId )
left outer join a_clickUserNum on(c.adId = a_clickUserNum.adId )
left outer join a_notClickUserNum on(c.adId = a_notClickUserNum.adId )
left outer join a_languageNum on(c.adId = a_languageNum.adId )
left outer join a_clickLanguageNum on(c.adId = a_clickLanguageNum.adId )
left outer join a_notClickLanguageNum on(c.adId = a_notClickLanguageNum.adId )
left outer join a_monitorNum on(c.adId = a_monitorNum.adId )
left outer join a_clickMonitorNum on(c.adId = a_clickMonitorNum.adId )
left outer join a_notClickMonitorNum on(c.adId = a_notClickMonitorNum.adId )
left outer join a_IPNum on(c.adId = a_IPNum.adId )
left outer join a_clickIPNum on(c.adId = a_clickIPNum.adId )
left outer join a_notClickIPNum on(c.adId = a_notClickIPNum.adId )
left outer join a_visDateNum on(c.adId = a_visDateNum.adId )
left outer join a_clickVisDateNum on(c.adId = a_clickVisDateNum.adId )
left outer join a_notClickVisDateNum on(c.adId = a_notClickVisDateNum.adId )
left outer join a_sysNum on(c.adId = a_sysNum.adId )
left outer join a_sysClickNum on(c.adId = a_sysClickNum.adId )
left outer join a_sysNotClickNum on(c.adId = a_sysNotClickNum.adId )
left outer join a_sysExplorNum on(c.adId = a_sysExplorNum.adId )
left outer join a_sysCilckExplorNum on(c.adId = a_sysCilckExplorNum.adId )
left outer join a_sysNotCilckExplorNum on(c.adId = a_sysNotCilckExplorNum.adId )
left outer join a_dateNum on(c.adId = a_dateNum.adId )
left outer join a_dateClickNum on(c.adId = a_dateClickNum.adId )
left outer join a_dateNotClickNum on(c.adId = a_dateNotClickNum.adId )
left outer join m_usermoni_Num on(c.userId = m_usermoni_Num.userId and c.monitorId = m_usermoni_Num.monitorId )
left outer join mu_clickNum on(c.userId = mu_clickNum.userId and c.monitorId = mu_clickNum.monitorId )
left outer join mu_notClickNum on(c.userId = mu_notClickNum.userId and c.monitorId = mu_notClickNum.monitorId )
left outer join ua_allNum on(c.userId = ua_allNum.userId and c.adId = ua_allNum.adId )
left outer join ua_clickNum on(c.userId = ua_clickNum.userId and c.adId = ua_clickNum.adId )
left outer join ua_notClickNum on(c.userId = ua_notClickNum.userId and c.adId = ua_notClickNum.adId );
--left outer join target on(c.userId = target.userId and c.monitorId = target.monitorId );