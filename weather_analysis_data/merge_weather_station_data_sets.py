import sys
import os
sys.path.append(os.path.abspath('..'))
from raw_weather_analysis_data.preprocess_raw_weather_data import preprocess
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from merged_weather_data_expected_schema import merged_weather_data_expected_schema

def mergeWeatherData(weather_data_dict):
  # Check if input is a dictionary with station names mapped to DataFrames
  if not isinstance(weather_data_dict, dict):
    raise ValueError("Input weather data must be a dictionary.")

  # Check if values in the dictionary are Spark DataFrames
  for key, value in weather_data_dict.items():
    if not isinstance(value, DataFrame):
      raise ValueError("Values in the dictionary must be Spark DataFrames.")

  merged_weather_data_set_sdf = None

  for station, weather_data_set_sdf in weather_data_dict.items():
    preprocess_weather_data_set_sdf = preprocess(weather_data_set_sdf) # transforms column names and data types
    weather_data_with_station_col_sdf = preprocess_weather_data_set_sdf.withColumn("station", lit(station)) # Add station name

    if merged_weather_data_set_sdf is None:
      merged_weather_data_set_sdf = weather_data_with_station_col_sdf
    else:
      merged_weather_data_set_sdf = merged_weather_data_set_sdf.union(weather_data_with_station_col_sdf)

  merged_weather_sdf = merged_weather_data_set_sdf.withColumn("station", when(col("station").isNotNull(),col("station")).otherwise(lit(None)))

  assert merged_weather_data_expected_schema == merged_weather_sdf.schema

  return merged_weather_sdf
