from pyspark.sql.types import StructType, StructField, DateType, StringType, LongType

maturity_area_seasonal_expected_schema = StructType([
  StructField("Maturity_Area", StringType(), True),
  StructField("KPIN", LongType(), True),
  StructField("season", LongType(), True),
  StructField("WeatherStation", StringType(), True),
  StructField("harvest_date", DateType(), True, {"__detected_date_formats":"d/M/yyyy"}),
  StructField("packhouse", StringType(), True),
])
