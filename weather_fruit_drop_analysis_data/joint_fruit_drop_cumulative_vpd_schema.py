from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, LongType

joint_fruit_drop_cumulative_vpd_schema = StructType([
  StructField("fruit_drop_assessment_date", DateType(), True, {"__detected_date_formats":"d/M/yyyy"}),
  StructField("station", StringType(), True),
  StructField("KPIN", LongType(), True),
  StructField("maturity_area", StringType(), True),
  StructField("bay", LongType(), True),
  StructField("cane_number", LongType(), True),
  StructField("fruit_drop_ratio", DoubleType(), True),
  StructField("cumulative_fruit_drop_ratio", DoubleType(), True),
  StructField("cumulative_vapour_pressure_deficit_stress", LongType(), True)
])
