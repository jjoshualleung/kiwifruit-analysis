from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType

joint_maturity_area_expected_schema = StructType([
  StructField("Maturity_Area", StringType(), True),
  StructField("KPIN", LongType(), True),
  StructField("Grower", StringType(), True),
  StructField("OP", StringType(), True),
  StructField("Region", StringType(), True),
  StructField("Latitude", DoubleType(), True),
  StructField("Longitude", DoubleType(), True),
  StructField("FD_HystoricalPresence", StringType(), True),
  StructField("season", LongType(), True),
  StructField("WeatherStation", StringType(), True),
  StructField("harvest_date", DateType(), True, {"__detected_date_formats":"d/M/yyyy"}),
  StructField("packhouse", StringType(), True)
])
