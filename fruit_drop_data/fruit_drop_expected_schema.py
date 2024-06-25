from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

fruit_drop_expected_schema = StructType([
  StructField('Grower', StringType(), True),
  StructField('KPIN', LongType(), True),
  StructField('Maturity Area', StringType(), True), 
  StructField('Bay', LongType(), True), 
  StructField('Cane number', LongType(), True), 
  StructField('assessment date', DateType(), True, {"__detected_date_formats":"d/M/yyyy"}), 
  StructField('cane position', StringType(), True), 
  StructField('shrinked fruit still attached on the cane', LongType(), True), 
  StructField('not dropped fruit with dry peduncle', LongType(), True), 
  StructField('dropped fruit with dry peduncle', LongType(), True), 
  StructField('dropped fruit with healthy peduncle', LongType(), True)
])
