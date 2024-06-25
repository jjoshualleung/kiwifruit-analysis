from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

maturity_area_orchard_expected_schema = StructType([
    StructField("Grower", StringType(), True),
    StructField("Maturity_Area", StringType(), True),
    StructField("OP", StringType(), True),
    StructField("KPIN", LongType(), True),
    StructField("Region", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("FD_HystoricalPresence", StringType(), True)
])