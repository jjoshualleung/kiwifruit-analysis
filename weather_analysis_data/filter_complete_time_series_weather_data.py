import os
import sys
sys.path.append(os.path.abspath('..'))
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from weather_analysis_data.merged_weather_data_expected_schema import merged_weather_data_expected_schema
from weather_analysis_data.complete_time_series_expected_schema import complete_time_series_expected_schema

def filterCompleteWeatherTimeSeries(merged_weather_sdf):

  assert merged_weather_data_expected_schema == merged_weather_sdf.schema

  filtered_weather_sdf = merged_weather_sdf.filter(
    col("avg_temperature_C").isNotNull() & col("avg_humidity_percent").isNotNull()
  )

  distinct_stations_sdf = merged_weather_sdf.select("station").distinct()
  distinct_stations_list = [row.station for row in distinct_stations_sdf.collect()]

  common_date_time_sdf = None

  for station in distinct_stations_list:
    station_date_time_sdf = filtered_weather_sdf.filter(col("station") == station).select("datetime")
    if common_date_time_sdf is None:
        common_date_time_sdf = station_date_time_sdf
    else:
      common_date_time_sdf = common_date_time_sdf.intersect(station_date_time_sdf)
  
  complete_timeseries_sdf = common_date_time_sdf

  assert complete_time_series_expected_schema == complete_timeseries_sdf.schema

  return complete_timeseries_sdf
