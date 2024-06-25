from pyspark.sql.window import Window
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, lit, to_date
from cumulative_weather_stress_schema import cumulative_weather_stress_schema

def compute_cumulative_weather_stress(complete_timeseries_weather_data_sdf):

  distinct_weather_datetime_sdf = complete_timeseries_weather_data_sdf.select("datetime", "station").distinct()
  
  window = Window.partitionBy("station").orderBy("datetime")

  compute_cumulative_weather_stress_sdf = distinct_weather_datetime_sdf.withColumn(
    "cumulative_weather_stress", 
    F.row_number().over(window)
  ).withColumn(
    "date", to_date("datetime")
  ).withColumn(
    "cumulative_weather_stress", 
    when(
      col("cumulative_weather_stress").isNotNull(), 
      col("cumulative_weather_stress")
    ).otherwise(lit(None)).cast(LongType())
  )

  cumulative_weather_stress_sdf = compute_cumulative_weather_stress_sdf.select(
    "date",
    "station", 
    "cumulative_weather_stress"
  )

  assert cumulative_weather_stress_schema == cumulative_weather_stress_sdf.schema

  return cumulative_weather_stress_sdf
