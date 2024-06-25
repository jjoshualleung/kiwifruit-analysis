import os
import sys
import unittest
sys.path.append(os.path.abspath('..'))
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, exp, round
from weather_analysis_data.vapour_pressure_deficit_schema import vapour_pressure_deficit_schema
from weather_analysis_data.merged_weather_data_expected_schema import merged_weather_data_expected_schema

def compute_vapour_pressure_deficit(weather_station_sdf, period_days):

  assert merged_weather_data_expected_schema == weather_station_sdf.schema
  assert isinstance(period_days, int) and period_days > 0, "period_days must be a positive integer greater than 0"

  extract_date = weather_station_sdf.withColumn("date", to_date("datetime"))

  daily_max_min_tem_humid_sdf = extract_date.groupBy("date", "station").agg(
    F.max("avg_temperature_C").alias("daily_maximum_avg_temperature_C"),
    F.min("avg_temperature_C").alias("daily_minimum_avg_temperature_C"),
    F.max("avg_humidity_percent").alias("daily_maximum_avg_humidity_percent"),
    F.min("avg_humidity_percent").alias("daily_minimum_avg_humidity_percent")
  )
  
  saturation_vapour_pressure_at_air_temp_T_sdf = daily_max_min_tem_humid_sdf.withColumn(
    "e_t_max", 0.6108 * exp(17.27 * col("daily_maximum_avg_temperature_C") / (col("daily_maximum_avg_temperature_C") + 237.3)) 
  ).withColumn(
    "e_t_min", 0.6108 * exp(17.27 * col("daily_minimum_avg_temperature_C") / (col("daily_minimum_avg_temperature_C") + 237.3))
  )
  
  mean_saturation_vapour_pressure_at_air_temp_T_sdf = saturation_vapour_pressure_at_air_temp_T_sdf.withColumn(
    "es", (col("e_t_max") + col("e_t_min"))/2
  )
  
  window = Window.partitionBy("station").orderBy("date").rowsBetween(-period_days + 1, 0)

  relative_humidity_sdf = mean_saturation_vapour_pressure_at_air_temp_T_sdf.withColumn(
      "rh_max", F.avg("daily_maximum_avg_humidity_percent").over(window)
  ).withColumn(
      "rh_min", F.avg("daily_minimum_avg_humidity_percent").over(window)
  )
  
  actual_vapour_pressure_sdf = relative_humidity_sdf.withColumn(
    "ea", ((col("e_t_min") * col("rh_max") / 100) + (col("e_t_max") * col("rh_min") / 100)) / 2
  )
  
  weather_station_with_vapour_pressure_sdf = actual_vapour_pressure_sdf

  vapour_pressure_deficit_sdf = weather_station_with_vapour_pressure_sdf.withColumn(
    "vpd", col("es") - col("ea")
  )

  assert vapour_pressure_deficit_schema == vapour_pressure_deficit_sdf.schema

  return vapour_pressure_deficit_sdf
