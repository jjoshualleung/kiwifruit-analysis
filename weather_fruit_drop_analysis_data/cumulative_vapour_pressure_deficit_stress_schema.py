from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

cumulative_vapour_pressure_deficit_stress_schema = StructType([
  StructField("date", DateType(), True),
  StructField("station", StringType(), True),
  StructField("cumulative_vapour_pressure_deficit_stress", LongType(), True)
])
