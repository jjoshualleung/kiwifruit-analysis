from pyspark.sql.functions import col, max, when
from pyspark.sql import functions as F
from joint_cane_level_fruit_drop_schema import joint_cane_level_fruit_drop_schema
from joint_fruit_drop_cumulative_vpd_schema import joint_fruit_drop_cumulative_vpd_schema
from cumulative_vapour_pressure_deficit_stress_schema import cumulative_vapour_pressure_deficit_stress_schema

def cross_reference_cane_level_fruit_drop_and_vpd_stress(joint_cane_level_fruit_drop_sdf, cumulative_vapour_pressure_deficit_stress_sdf):

  assert joint_cane_level_fruit_drop_schema == joint_cane_level_fruit_drop_sdf.schema
  assert cumulative_vapour_pressure_deficit_stress_schema == cumulative_vapour_pressure_deficit_stress_sdf.schema

  join_max_vpd_stress_date_sdf = joint_cane_level_fruit_drop_sdf.join(
    cumulative_vapour_pressure_deficit_stress_sdf,
    (joint_cane_level_fruit_drop_sdf["fruit_drop_assessment_date"] >= cumulative_vapour_pressure_deficit_stress_sdf["date"]) &
    (joint_cane_level_fruit_drop_sdf["WeatherStation"] == cumulative_vapour_pressure_deficit_stress_sdf["station"]),
    "left"
    ).groupBy(
      *joint_cane_level_fruit_drop_sdf.columns,
    ).agg(
      F.max("cumulative_vapour_pressure_deficit_stress").alias("cumulative_vapour_pressure_deficit_stress")
    ).withColumnRenamed("WeatherStation", "station")

  joint_fruit_drop_cumulative_vpd_stress_sdf = join_max_vpd_stress_date_sdf.select(
    "fruit_drop_assessment_date",
    "station",
    "KPIN",
    "maturity_area",
    "bay",
    "cane_number",
    "fruit_drop_ratio",
    "cumulative_fruit_drop_ratio",
    "cumulative_vapour_pressure_deficit_stress"
  )

  assert joint_fruit_drop_cumulative_vpd_schema == joint_fruit_drop_cumulative_vpd_stress_sdf.schema

  return joint_fruit_drop_cumulative_vpd_stress_sdf
