#Model Data Preparations
#Creating temp table (outage_weather) to be able to join with weather table


library(SparkR)
library(sparklyr)

sc <- spark_connect(method = "databricks")
outage = sdf_sql(sc, "select * from group_2.outage")
outage

class(outage)
colnames(outage)

library(dplyr)
library(lubridate)

wea = sdf_sql(sc, "select * from ppl_data.weather")
colnames(wea)

wea2 = wea%>% select( TIME_GMT_FK ,WIND_GUST_MPH) %>% mutate( TIME_GMT_FK = as.character(TIME_GMT_FK) ) %>% mutate(TIME_GMT = to_date(TIME_GMT_FK ,'yyyyMMdd'))%>% mutate(MO = month(TIME_GMT), YR = year(TIME_GMT))
wea2

wea2 %>% group_by(MO,YR)%>% summarize( Max_wind = max(WIND_GUST_MPH))

Cummulative_Snow = wea%>% select( METAR_ID,TIME_GMT_FK ,SNOWFALL_PAST_1HOUR_IN) %>% mutate( TIME_GMT_FK = as.character(TIME_GMT_FK) ) %>% mutate(TIME_GMT = to_date(TIME_GMT_FK ,'yyyyMMdd'))%>% group_by(METAR_ID,TIME_GMT)%>% summarize(day_snow = sum(SNOWFALL_PAST_1HOUR_IN))%>% arrange (METAR_ID, TIME_GMT) %>% mutate(three_day_snow = day_snow + lag(day_snow,1,default = 0) + lag(day_snow,2, default =0))
Cummulative_Snow%>%filter(three_day_snow>0)

weather_loc = wea %>% distinct(METAR_ID,LATITUDE, LONGITUDE)
weather_loc

outage_loc = outage  %>% filter (LATITUDE !=0 , LONGITUDE !=0)%>%distinct(FAILED_FACILITY_ID,LATITUDE, LONGITUDE)
outage_loc  %>% distinct(LATITUDE, LONGITUDE) %>% summarize(total = n())
outage_loc  %>% distinct(FAILED_FACILITY_ID) %>% summarize(total = n())
outage_loc = outage %>% filter (LATITUDE !=0 , LONGITUDE !=0)%>%distinct(LATITUDE, LONGITUDE) %>% arrange(LATITUDE,LATITUDE) %>% mutate(LOC_ID= row_number()) %>%  select (LOC_ID,LATITUDE,LONGITUDE )
outage_loc  %>% distinct(LOC_ID) %>% summarize(total = n())
outage_loc

library(RANN)
library(geosphere)

weather_loc_df = collect(weather_loc)
outage_loc_df = collect(outage_loc)
