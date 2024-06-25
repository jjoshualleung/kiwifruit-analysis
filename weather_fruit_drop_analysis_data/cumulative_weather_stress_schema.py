from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

cumulative_weather_stress_schema = StructType([
  StructField("date", DateType(), True),
  StructField("station", StringType(), True),
  StructField("cumulative_weather_stress", LongType(), True)
])
