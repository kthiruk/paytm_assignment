from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import logging
from datetime import datetime

def __initialize_logger(sc):
    log4jLogger = sc._jvm.org.apache.log4j
    return log4jLogger.LogManager.getLogger("WeatherData:Logger")

def __get_props():
    with open('C://data/WeatherDataLocal.json') as f:
        config_data = json.load(f)
    return config_data

if __name__ == '__main__':
    spark = SparkSession.builder.appName('WeatherData').getOrCreate()
    sc = spark.sparkContext

    config_data = __get_props()
    logger = __initialize_logger(sc)

    inbound_dir = config_data['inbound_dir']

    try:
        logger.info("Weather Streaming Application has started at %s" % datetime.now())
        weather_data = spark.read.format("csv").option('header','true').load("C:/data/paytm/paytmteam-de-weather-challenge-b01d5ebbf02d/data/2019")
		country_list = spark.read.format("csv").option('header','true').load('C:/data/paytm/paytmteam-de-weather-challenge-b01d5ebbf02d/lookups/country')
		station_list = spark.read.format("csv").option('header','true').load('C:/data/paytm/paytmteam-de-weather-challenge-b01d5ebbf02d/lookups/stationlist')

		weather_data.createOrReplaceTempView('wd')
		country_list.createOrReplaceTempView('cl')
		station_list.createOrReplaceTempView('sl')

		station_with_country = spark.sql("select a.STN_NO, a.country_abbr, b.country_full from sl a inner join cl b on a.country_abbr = b.country_abbr")
		station_with_country.createOrReplaceTempView('swc')

		weather_data_with_country = spark.sql("select `STN---`,WBAN,YEARMODA,TEMP,DEWP,SLP,STP, VISIB, WDSP,MXSPD,GUST, MAX, MIN, PRCP,SNDP,FRSHTT,  b.country_full from wd w left outer join swc b on `STN---` = STN_NO")
		weather_data_with_country.createOrReplaceTempView('wwc')
		weather_data_with_country.persist()

		results = spark.sql("select year, country_full, avg_temp, avg_windspeed,  \
		rank() over(partition by year order by avg_temp) as lowest_temp_rank , \
		rank() over(partition by year order by avg_temp desc) as highest_temp_rank, \
		rank() over(partition by year order by avg_windspeed desc ) as highest_windspeed_rank from   \
		( select year(to_date(YEARMODA,'yyyyMMdd')) as year, country_full, avg(case when TEMP = 9999.9 then 0 else TEMP end) as avg_temp, \
		avg(case when WDSP = 999.9 then 0 else WDSP end) as avg_windspeed \
		from wwc group by year(to_date(YEARMODA,'yyyyMMdd')), country_full) a")

		results.createOrReplaceTempView('res')

		print("Below is the Countries with the Highest Average Temparatures : ")
		spark.sql("select year, country_full, avg_temp, highest_temp_rank from res where highest_temp_rank = 1").show()

		print("Below is the Countries with the Lowest Average Temparatures : ")
		spark.sql("select year, country_full, avg_temp, lowest_temp_rank  from res where lowest_temp_rank = 1").show()

		print("Below is the Countries with the highest Wind Speed : ")
		spark.sql("select year, country_full, avg_windspeed, highest_windspeed_rank from res where highest_windspeed_rank = 2").show()

		tornadoes = spark.sql("with cte_temp as (select year, yearmoda,country_full, \
		date_sub(to_date(yearmoda,'yyyyMMdd'),row_number() over(partition by country_full order by yearmoda)) as deter from \
		( select distinct year(to_date(yearmoda,'yyyyMMdd')) as year ,yearmoda, country_full from wwc where substring(FRSHTT,6,1) = 1) a) \
		select year, country_full ,cnt as count_in_days, rank() over (partition by year order by cnt desc ) as most_consec_days from \
		(select year, country_full, deter, count(*) as cnt from cte_temp group by year, country_full, deter) a")

		tornadoes.createOrReplaceTempView('tornadoes')

		print("Below is the Countries with the Most Consecutive Tornado Days : ")
		spark.sql("select year, country_full from (select year, country_full, rank() over (partition by year order by cnt desc) as rn from (select year, country_full, count(*) as cnt from tornadoes where most_consec_days = 1 group by year, country_full) a) b where rn = 1").show()

    except Exception as e:
        logger.error("Weather Streaming Application has failed at %s"  % datetime.now())
        logger.error("Exception: {}".format(e))
        logger.error("Exception Type : {}".format(type(e).__name__))
        logger.error("Exception Arguments : {}".format(e.args))
		


