from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lag, when, coalesce
from joint_cane_level_fruit_drop_schema import joint_cane_level_fruit_drop_schema

def compute_joint_cane_level_fruit_drop_ratio(joint_cane_level_fruit_drop_sdf):

  window = Window.partitionBy("WeatherStation", "grower", "maturity_area", "bay", "cane_number").orderBy("fruit_drop_assessment_date")

  remaining_fruit_sdf = joint_cane_level_fruit_drop_sdf.withColumn("remaining_fruit", F.col("initial_fruit_number") - F.sum("total_dropped_dry_and_healthy_peduncle").over(window))

  prev_remaining_fruit_sdf = remaining_fruit_sdf.withColumn("prev_remaining_fruit", F.lag("remaining_fruit").over(window))

  set_initial_fruit_number = F.col("initial_fruit_number")
  update_prev_remaining_fruit_sdf = prev_remaining_fruit_sdf.withColumn("prev_remaining_fruit", F.coalesce(F.col("prev_remaining_fruit"), set_initial_fruit_number))

  fruit_drop_ratio_sdf = update_prev_remaining_fruit_sdf.withColumn(
    "fruit_drop_ratio", 
    (F.col("prev_remaining_fruit") - F.col("remaining_fruit")) / F.col("prev_remaining_fruit")
  ).withColumn(
    "cumulative_fruit_drop_ratio",
    (F.col("initial_fruit_number") - F.col("remaining_fruit"))/ F.col("initial_fruit_number")
  )

  joint_cane_level_fruit_drop_sdf = fruit_drop_ratio_sdf.select(
    "fruit_drop_assessment_date",
    "WeatherStation",
    "grower",
    "KPIN",
    "maturity_area",
    "bay",
    "cane_number",
    "initial_fruit_number",
    "total_dropped_dry_and_healthy_peduncle",
    "prev_remaining_fruit",
    "remaining_fruit",
    "fruit_drop_ratio",
    "cumulative_fruit_drop_ratio"
  )

  assert joint_cane_level_fruit_drop_schema == joint_cane_level_fruit_drop_sdf.schema

  return joint_cane_level_fruit_drop_sdf