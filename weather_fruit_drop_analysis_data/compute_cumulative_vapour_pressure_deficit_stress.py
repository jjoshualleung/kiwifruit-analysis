from pyspark.sql.window import Window
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, lit, to_date
from cumulative_vapour_pressure_deficit_stress_schema import cumulative_vapour_pressure_deficit_stress_schema

def compute_cumulative_vapour_pressure_deficit_stress(vapour_pressure_deficit_stress_with_weather_data_sdf):

  distinct_weather_date_sdf = vapour_pressure_deficit_stress_with_weather_data_sdf.select("date", "station").distinct()
  
  window = Window.partitionBy("station").orderBy("date")

  compute_cumulative_vpd_stress_sdf = distinct_weather_date_sdf.withColumn(
    "cumulative_vapour_pressure_deficit_stress", 
    F.row_number().over(window)
  ).withColumn(
    "cumulative_vapour_pressure_deficit_stress", 
    when(
      col("cumulative_vapour_pressure_deficit_stress").isNotNull(), 
      col("cumulative_vapour_pressure_deficit_stress")
    ).otherwise(lit(None)).cast(LongType())
  )

  cumulative_vpd_stress_sdf = compute_cumulative_vpd_stress_sdf.select(
    "date",
    "station", 
    "cumulative_vapour_pressure_deficit_stress"
  )

  assert cumulative_vapour_pressure_deficit_stress_schema == cumulative_vpd_stress_sdf.schema

  return cumulative_vpd_stress_sdf
