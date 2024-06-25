from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

merged_weather_data_expected_schema = StructType([
  StructField("datetime", TimestampType(), True),
  StructField("rain_mm", DoubleType(), True),
  StructField("max_temperature_C", DoubleType(), True),
  StructField("min_temperature_C", DoubleType(), True),
  StructField("avg_temperature_C", DoubleType(), True),
  StructField("max_humidity_percent", DoubleType(), True),
  StructField("min_humidity_percent", DoubleType(), True),
  StructField("avg_humidity_percent", DoubleType(), True),
  StructField("solarrad_MJ_per_m2", DoubleType(), True),
  StructField("windspeed_m_per_s", DoubleType(), True),
  StructField("windgusts_m_per_s", DoubleType(), True),
  StructField("station", StringType(), True)
])
