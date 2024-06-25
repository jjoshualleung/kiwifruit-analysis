from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType

joint_cane_level_fruit_drop_schema = StructType([
  StructField("fruit_drop_assessment_date", DateType(), True, {"__detected_date_formats":"d/M/yyyy"}),
  StructField("WeatherStation", StringType(), True),
  StructField("grower", StringType(), True), 
  StructField("KPIN", LongType(), True), 
  StructField("maturity_area", StringType(), True), 
  StructField("bay", LongType(), True), 
  StructField("cane_number", LongType(), True), 
  StructField("initial_fruit_number", LongType(), True),
  StructField("total_dropped_dry_and_healthy_peduncle", LongType(), True),
  StructField("prev_remaining_fruit", LongType(), True),
  StructField("remaining_fruit", LongType(), True),
  StructField("fruit_drop_ratio", DoubleType(), True),
  StructField("cumulative_fruit_drop_ratio", DoubleType(), True)
])
