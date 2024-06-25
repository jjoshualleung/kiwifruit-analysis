from pyspark.sql.types import StructType, StructField, TimestampType

complete_time_series_expected_schema = StructType([
  StructField("datetime", TimestampType(), True),
])
