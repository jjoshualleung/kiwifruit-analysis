from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

vapour_pressure_deficit_schema = StructType([
  StructField("date", DateType(), True),
  StructField("station", StringType(), True),
  StructField("daily_maximum_avg_temperature_C", DoubleType(), True),
  StructField("daily_minimum_avg_temperature_C", DoubleType(), True),
  StructField("daily_maximum_avg_humidity_percent", DoubleType(), True),
  StructField("daily_minimum_avg_humidity_percent", DoubleType(), True),
  StructField("e_t_max", DoubleType(), True),
  StructField("e_t_min", DoubleType(), True),
  StructField("es", DoubleType(), True),
  StructField("rh_max", DoubleType(), True),
  StructField("rh_min", DoubleType(), True),
  StructField("ea", DoubleType(), True),
  StructField("vpd", DoubleType(), True)
])
