-- DBTITLE 1,Feature Engineering
-- With the aim to get the underlying weather conditions that lead to outage, we transformed 9 features from weather table into 137 features to find important features;
-- we created these features by producing lags, maxes, averages, and sums of the original 9 attributes.
-- These include wind gust, cloud amount, relative humidity, dewpoint temperature, air temperature, precipitation, snow fall, wind speed and atmospheric pressure.
-- To get the most dire levels that lead to outage, we took a summation of snowfall and precipitation on an hourly basis, averaged the max levels for the rest of the features on a daily basis.

use ppl_data

select * from weather


create or replace view daily_weather as
 select latitude as w_latitude, longitude as w_longitude,
 to_date( cast(TIME_GMT_FK as char(8)) ,'yyyyMMdd')  as event_date,

 max(wind_gust_mph) as max_wind_gust_daily,
 max(cloud_amount) as max_cloud_amount_daily,

 max(relative_humidity) as max_humidity_daily,
 max(DEWPOINT_TEMPERATURE_F) as max_dewpoint_tempf_daily,
 max(AIR_TEMPERATURE_F) as max_air_tempf_daily,

 max(PRECIPITATION_PAST_1HOUR_IN) as max_precipitation_daily,
 max(wind_speed_mph) as max_wind_speed_daily,
 max(atmospheric_pressure_mb) as max_atmospheric_pressure_daily,

 sum(SNOWFALL_PAST_1HOUR_IN) as total_snow_daily, avg(wind_gust_mph) as avg_wind_gust_daily, avg(cloud_amount) as avg_cloud_amount_daily,

 sum(PRECIPITATION_PAST_1HOUR_IN) as total_precipitation_daily, avg(wind_speed_mph) as avg_wind_speed_daily, avg(atmospheric_pressure_mb) as avg_atmospheric_pressure_daily,

 avg(relative_humidity) as avg_humidity_daily, avg(DEWPOINT_TEMPERATURE_F) as avg_dewpoint_tempf_daily, avg(AIR_TEMPERATURE_F) as avg_air_tempf_daily

 from weather group by 1,2,3



create or replace view Group_2.All_Features as  select w_latitude, w_longitude, event_date,

max_wind_gust_daily, max_cloud_amount_daily,
max_humidity_daily, max_dewpoint_tempf_daily, max_air_tempf_daily,
max_precipitation_daily, max_wind_speed_daily, max_atmospheric_pressure_daily,

total_snow_daily, avg_wind_gust_daily, avg_cloud_amount_daily,
total_precipitation_daily, avg_wind_speed_daily, avg_atmospheric_pressure_daily,
avg_humidity_daily, avg_dewpoint_tempf_daily, avg_air_tempf_daily,

--weather_features_tmp_ca

MAX(max_wind_gust_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_wind_gust_l3d,
MAX(max_wind_gust_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_wind_gust_l7d,
MAX(max_wind_gust_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_wind_gust_l15d,
MAX(max_wind_gust_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_wind_gust_l30d,
MAX(max_wind_gust_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_wind_gust_l60d,
MAX(max_wind_gust_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_wind_gust_l90d,
MAX(max_cloud_amount_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_cloud_amount_l3d,
MAX(max_cloud_amount_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_cloud_amount_l7d,
MAX(max_cloud_amount_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_cloud_amount_l15d,
MAX(max_cloud_amount_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_cloud_amount_l30d,
MAX(max_cloud_amount_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_cloud_amount_l60d,
MAX(max_cloud_amount_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_cloud_amount_l90d,
max(max_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_humidity_l3d,
max(max_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_humidity_l7d,
max(max_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_humidity_l15d,
max(max_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_humidity_l30d,
max(max_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_humidity_l60d,
max(max_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_humidity_l90d,
max(max_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_dewpoint_tempf_l3d,
max(max_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_dewpoint_tempf_l7d,
max(max_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_dewpoint_tempf_l15d,
max(max_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_dewpoint_tempf_l30d,
max(max_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_dewpoint_tempf_l60d,
max(max_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_dewpoint_tempf_l90d,
max(max_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_air_tempf_l3d,
max(max_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_air_tempf_l7d,
max(max_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_air_tempf_l15d,
max(max_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_air_tempf_l30d,
max(max_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_air_tempf_l60d,
max(max_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_air_tempf_l90d,
max(max_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_precipitation_l3d,
max(max_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_precipitation_l7d,
max(max_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_precipitation_l15d,
max(max_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_precipitation_l30d,
max(max_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_precipitation_l60d,
max(max_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_precipitation_l90d,
max(max_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_wind_speed_l3d,
max(max_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_wind_speed_l7d,
max(max_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_wind_speed_l15d,
max(max_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_wind_speed_l30d,
max(max_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_wind_speed_l60d,
max(max_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_wind_speed_l90d,
max(max_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as max_atm_pressure_l3d,
max(max_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as max_atm_pressure_l7d,
max(max_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as max_atm_pressure_l15d,
max(max_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as max_atm_pressure_l30d,
max(max_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as max_atm_pressure_l60d,
max(max_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as max_atm_pressure_l90d,
sum(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as total_snow_l3d,
sum(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as total_snow_l7d,
sum(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as total_snow_l15d,
sum(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as total_snow_l30d,
sum(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as total_snow_l60d,
sum(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as total_snow_l90d,
avg(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_snow_l3d,
avg(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_snow_l7d,
avg(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_snow_l15d,
avg(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_snow_l30d,
avg(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_snow_l60d,
avg(total_snow_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_snow_l90d,
sum(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as total_precipitation_l3d,
sum(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as total_precipitation_l7d,
sum(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as total_precipitation_l15d,
sum(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as total_precipitation_l30d,
sum(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as total_precipitation_l60d,
sum(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as total_precipitation_l90d,
avg(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_precipitation_l3d,
avg(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_precipitation_l7d,
avg(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_precipitation_l15d,
avg(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_precipitation_l30d,
avg(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_precipitation_l60d,
avg(total_precipitation_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_precipitation_l90d,
avg(avg_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_wind_gust_l3d,
avg(avg_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_wind_gust_l7d,
avg(avg_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_wind_gust_l15d,
avg(avg_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_wind_gust_l30d,
avg(avg_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_wind_gust_l60d,
avg(avg_wind_speed_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_wind_gust_l90d,
avg(avg_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_cloud_amount_l3d,
avg(avg_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_cloud_amount_l7d,
avg(avg_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_cloud_amount_l15d,
avg(avg_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_cloud_amount_l30d,
avg(avg_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_cloud_amount_l60d,
avg(avg_atmospheric_pressure_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_cloud_amount_l90d,
avg(avg_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_humidity_l3d,
avg(avg_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_humidity_l7d,
avg(avg_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_humidity_l15d,
avg(avg_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_humidity_l30d,
avg(avg_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_humidity_l60d,
avg(avg_humidity_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_humidity_l90d,
avg(avg_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_dewpoint_tempf_l3d,
avg(avg_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_dewpoint_tempf_l7d,
avg(avg_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_dewpoint_tempf_l15d,
avg(avg_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_dewpoint_tempf_l30d,
avg(avg_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_dewpoint_tempf_l60d,
avg(avg_dewpoint_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_dewpoint_tempf_l90d,
avg(avg_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 2 following) as avg_air_tempf_l3d,
avg(avg_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 6 following) as avg_air_tempf_l7d,
avg(avg_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 14 following) as avg_air_tempf_l15d,
avg(avg_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 29 following) as avg_air_tempf_l30d,
avg(avg_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 59 following) as avg_air_tempf_l60d,
avg(avg_air_tempf_daily) over (partition by w_latitude, w_longitude order by event_date desc rows between CURRENT ROW and 89 following) as avg_air_tempf_l90d

from daily_weather

select * from Group_2.All_Features


-- MAGIC ### 5.3 Join temp tables

create  table Group_2.model_data as
select ot.dateGmt, ot.latitude,
ot.cause_code,
af.* , ot.target
from Group_2.outage_target ot
inner join
Group_2.outage_weather ow on (ot.latitude=ow.latitude and ot.longitude=ow.longitude)
left join
Group_2.All_Features af on (ow.w_latitude=af.w_latitude and ow.w_longitude=af.w_longitude and ot.dateGMT=af.event_date )



-- MAGIC ### 5.4 Categorical variable - Imputing with WOE value : LN ( % of non-events / % of events)
-- MAGIC We imputed the categorical variable "cause" from the outage table using a Weight of Evidence Value.

select
cause_code, sum(target), count(*) from Group_2.model_data
group by cause_code

-- MAGIC ### 5.5 Create final model table

create table Group_2.model_data_woe as
select
case when  cause_code=40 then -0.271773868643914 else
case when cause_code=41 then -0.510125305145586 else
case when  cause_code=77 then 0.96428135529315 else
case when cause_code=78 then 0.339466340093115 else
case when cause_code=30 then 1.0480667447059 else
case when  cause_code='??' then -0.533732248726246 else
case when  cause_code=96 then 1.29080377377377 else
case when  cause_code=80 then 1.36910289685041 else
case when  cause_code ='Other'  then -0.161013133588553 else
case when  cause_code=60 then -1.19435517787829
else -0.509400148066715 end end end end end end end end end end as cause_woe,
t.*
from Group_2.model_data t
