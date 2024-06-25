from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, LongType

joint_fruit_drop_cumulative_weather_stress_schema = StructType([
  StructField("fruit_drop_assessment_date", DateType(), True, {"__detected_date_formats":"d/M/yyyy"}),
  StructField("station", StringType(), True),
  StructField("KPIN", LongType(), True),
  StructField("maturity_area", StringType(), True),
  StructField("bay", LongType(), True),
  StructField("cane_number", LongType(), True),
  StructField("fruit_drop_ratio", DoubleType(), True),
  StructField("cumulative_fruit_drop_ratio", DoubleType(), True),
  StructField("cumulative_weather_stress", LongType(), True)
])

def validate_joint_fruit_drop_cumulative_weather_stress_sdf(joint_fruit_drop_cumulative_weather_stress_sdf):
  # Check for unque records
  unqiue_records_sdf = joint_fruit_drop_cumulative_weather_stress_sdf.groupBy(
    "fruit_drop_assessment_date",
    "station",
    "KPIN",
    "maturity_area",
    "bay",
    "cane_number"
  ).count().filter("count > 1")

  # Check if fruit_drop_ratio and cumulative_fruit_drop_ratio are negative or greater than 1
  fruit_drop_ratio_invalid_sdf = joint_fruit_drop_cumulative_weather_stress_sdf.filter(
    (col("fruit_drop_ratio") < 0) | 
    (col("fruit_drop_ratio") > 1) |
    (col("cumulative_fruit_drop_ratio") < 0)|
    (col("cumulative_fruit_drop_ratio") > 1) |
    (col("cumulative_fruit_drop_ratio") < col("fruit_drop_ratio"))
  )

  # Check if cumulative weather stress is negative
  cumulative_weather_stress_invalid_sdf = joint_fruit_drop_cumulative_weather_stress_sdf.filter(
    "cumulative_weather_stress <= 0"
  )

  invalid_records_sdf = None

  if unqiue_records_sdf.count() > 0:
    invalid_records_sdf = unqiue_records_sdf

  if fruit_drop_ratio_invalid_sdf.count() > 0:
    if invalid_records_sdf is None:
        invalid_records_sdf = fruit_drop_ratio_invalid_sdf
    else:
        invalid_records_sdf = invalid_records_sdf.union(fruit_drop_ratio_invalid_sdf)

  if cumulative_weather_stress_invalid_sdf.count() > 0:
    if invalid_records_sdf is None:
      invalid_records_sdf = cumulative_weather_stress_invalid_sdf
    else:
      invalid_records_sdf = invalid_records_sdf.union(cumulative_weather_stress_invalid_sdf)

  return invalid_records_sdf
