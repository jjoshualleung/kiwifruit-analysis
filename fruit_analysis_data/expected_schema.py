from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType

expected_schema = StructType(
  [
  StructField("Lab", StringType(), True),
  StructField("Provider", StringType(), True),
  StructField("Grower Number", LongType(), True),
  StructField("Orchard Name", StringType(), True),
  StructField("Variety", StringType(), True),
  StructField("Maturity Area Name", StringType(), True),
  StructField("Tray Estimate", StringType(), True),
  StructField("Sample Id", LongType(), True),
  StructField("SampleType", StringType(), True),
  StructField("M_Latest", StringType(), True),
  StructField("specialInstructions", StringType(), True),
  StructField("Fruit No.", LongType(), True),
  StructField("Collect Date", DateType(), True,{"__detected_date_formats":"d/M/yyyy"}),
  StructField("Full Bloom Date", StringType(), True),
  StructField("hidden", StringType(), True),
  StructField("Dry Matter", DoubleType(), True),
  StructField("Brix Equatorial", DoubleType(), True),
  StructField("Black Seeds", StringType(), True),
  StructField("Fresh Weight", DoubleType(), True),
  StructField("Hue", DoubleType(), True),
  StructField("Pressure", DoubleType(), True),
  StructField("Brix When Fully Ripe", StringType(), True)
  ]
)
