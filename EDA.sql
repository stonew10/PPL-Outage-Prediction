use ppl_data
select * from  group_2.outage

select RIGHT (TROUBLE_TIME,2) As YEAR, sum(CUSTOMERS_INTERRUPTED) , INCIDENT_STATUS
from group_2.outage
where INCIDENT_STATUS='Restored'
GROUP BY YEAR, CUSTOMERS_INTERRUPTED,INCIDENT_STATUS

select TROUBLE_TIME, INCIDENT_STATUS, count(*) as Incidentcount
from group_2.outage
where INCIDENT_STATUS='Restored'
Group by INCIDENT_STATUS, TROUBLE_TIME
ORDER BY Incidentcount DeSC

-- DBTITLE 1,Top 5 Incidents
select cause, INCIDENT_STATUS, count(*) as Incidentcount
from group_2.outage
where INCIDENT_STATUS='Restored'
Group by cause, INCIDENT_STATUS
ORDER BY Incidentcount DESC
Limit 5

select count (*) from weather
select * from weather


SELECT
   TIME_GMT,
   MAX (WIND_GUST_MPH)
FROM
   weather
GROUP BY
   TIME_GMT
ORDER BY TIME_GMT ASC


SELECT
   TIME_GMT,
   SUM (SNOWFALL_PAST_1HOUR_IN) Over (Order By TIME_GMT ASC) as RunningTotal
FROM
   weather
GROUP BY
   TIME_GMT, SNOWFALL_PAST_1HOUR_IN
ORDER BY TIME_GMT ASC


-- DBTITLE 1,4- Target Data Prep
create table Group_2.tmp1_target_0 as
select latitude, longitude , trouble_time_fk, to_date( cast(trouble_time_fk as char(8)) ,'yyyyMMdd') as dateGMT , cause_code, cause
from outage where incident_status='No Outage'
group by latitude, longitude , trouble_time_fk,cause_code, cause

create table Group_2.tmp1_target_1 as
select latitude, longitude , trouble_time_fk, to_date( cast(trouble_time_fk as char(8)) ,'yyyyMMdd')  as dateGMT,
date_add(to_date( cast(trouble_time_fk as char(8)) ,'yyyyMMdd'),1)  as dateGMT_1,  cause_code, cause
from outage where incident_status='Restored'
group by latitude, longitude, trouble_time_fk, cause_code, cause

create table Group_2.outage_target as
select t0.latitude,t0.longitude,max(t0.dateGMT) dateGMT, t2.cause_code, max(case when t2.target is null then 0 else t2.target end) as target from
(select t.latitude,t.longitude, t.dateGMT   from Group_2.tmp1_target_0 t
inner  join
Group_2.tmp1_target_1 t1 on (t.latitude=t1.latitude and t.longitude=t1.longitude) where  t1.dateGMT >=  t.dateGMT group by t.latitude,t.longitude , t.dateGMT ) t0
left join
Group_2.tmp_target_1 t2 on (t0.latitude=t2.latitude and t0.longitude=t2.longitude)
group by t0.latitude,t0.longitude, t2.cause_code

select * from Group_2.outage_target

select target, count(*) from Group_2.outage_target group by target
