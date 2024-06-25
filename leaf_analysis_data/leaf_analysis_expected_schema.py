from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType

expected_schema = StructType(
  [
  StructField("Region", StringType(), True),
  StructField("Sample time", StringType(), True),
  StructField("KPIN", LongType(), True),
  StructField("orchard", StringType(), True),
  StructField("MA", StringType(), True),
  StructField("Blk", StringType(), True),
  StructField("data analysis", DateType(), True,{"__detected_date_formats":"d/M/yy"}),
  StructField("N %", DoubleType(), True),
  StructField("P %", DoubleType(), True),
  StructField("K %", DoubleType(), True),
  StructField("Ca %", DoubleType(), True),
  StructField("Mg %", DoubleType(), True),
  StructField("Na %", DoubleType(), True),
  StructField("Fe ppm", LongType(), True),
  StructField("Mn ppm", LongType(), True),
  StructField("Cu ppm", LongType(), True),
  StructField("Zn ppm", LongType(), True),
  StructField("B ppm", LongType(), True)
]
)
