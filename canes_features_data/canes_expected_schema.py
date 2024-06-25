from pyspark.sql.types import DoubleType, StructType, StructField, StringType, LongType, DateType

canes_expected_schema = StructType([
  StructField("grower", StringType(), True), 
  StructField("KPIN", LongType(), True), 
  StructField("maturity_area", StringType(), True), 
  StructField("bay", LongType(), True), 
  StructField("latitude", DoubleType(), True), 
  StructField("longitude", DoubleType(), True), 
  StructField("sq_meters_bay", DoubleType(), True), 
  StructField("cane_length_cm", DoubleType(), True), 
  StructField("cane_number", LongType(), True), 
  StructField("cane_diameter_bottom_cm", DoubleType(), True), 
  StructField("cane_diameter_middle_cm", DoubleType(), True), 
  StructField("cane_diameter_top_cm", DoubleType(), True), 
  StructField("king_fruit_number", LongType(), True), 
  StructField("lateral_fruit_number", LongType(), True), 
  StructField("initial_fruit_number", LongType(), True), 
  StructField("assessment_date", DateType(), True, {"__detected_date_formats":"d/M/yyyy"})
])
