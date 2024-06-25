from pyspark.sql.types import DoubleType
from raw_weather_analysis_data.raw_weather_data_expected_schema import expected_schema
from pyspark.sql.functions import regexp_replace, to_timestamp, col

def preprocess(initial_sdf):
  # Rename columns
  new_column_names = [field.name for field in expected_schema.fields]
  
  for i, new_name in enumerate(new_column_names):
    initial_sdf = initial_sdf.withColumnRenamed(initial_sdf.columns[i], new_name)

  # Remove double hyphens to empty string
  remove_hyphens_sdf = initial_sdf.select([regexp_replace(c, "--", "").alias(c) for c in initial_sdf.columns])

  # Convert "date" to DateType
  convert_date_sdf = remove_hyphens_sdf.withColumn("datetime", to_timestamp("datetime", "d/M/yyyy HH:mm"))

  # Convert non-datetype columns to DoubleType
  convert_doubletype_sdf = convert_date_sdf
  for col_name in convert_doubletype_sdf.columns:
    if col_name != "datetime":
      convert_doubletype_sdf = convert_doubletype_sdf.withColumn(col_name, col(col_name).cast(DoubleType()))

  assert expected_schema == convert_doubletype_sdf.schema

  return convert_doubletype_sdf
