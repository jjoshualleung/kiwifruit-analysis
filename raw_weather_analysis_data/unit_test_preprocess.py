import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from preprocess_raw_weather_data import preprocess
from raw_weather_data_expected_schema import expected_schema

class TestPreprocess(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestPreprocess").getOrCreate()

  # FIXME:test suite does not complete when tearDown is present 
  # def tearDown(self):
  #   self.spark.stop()

  def test_3_records_with_incorrect_schema_and_column_names(self):
    test_data = [
    ("1/1/2023 00:00", "1.0", "20.0", "10.0", "15.0", "90.0", "100.0", "95.0", "1.0", "1.0", "100.0"), # day/month/year
    ("2/2/2023 01:00", "1", "20", "10", "15", "90", "100", "95", "1", "1", "100"),
    ("3/3/2023 02:00", "2.0", "--", "10.0", "15.0", "90.0", "--", "95.0", "1.0", "1.0", "100.0"),
    ]
    
    initial_sdf = self.spark.createDataFrame(
      test_data,
      [
        "datetime", "Rain (mm)", "Max Temperature (�C)", " Min Temperature(�C)", "AVG Temperature(�C)",
        "Max Humidity(%)", "MinHumidity(%)", "AVG Humidity(%)", 
        "SolarRad (MJ/m�)", "WindSpeed(m/s)", "WindGusts(m/s)",
      ]
    )
    
    result_df = preprocess(initial_sdf)

    expected_data = [
      (datetime(2023, 1, 1, 0, 0, 0), 1.0, 20.0, 10.0, 15.0, 90.0, 100.0, 95.0, 1.0, 1.0, 100.0),  # year/month/day
      (datetime(2023, 2, 2, 1, 0, 0), 1.0, 20.0, 10.0, 15.0, 90.0, 100.0, 95.0, 1.0, 1.0, 100.0),
      (datetime(2023, 3, 3, 2, 0, 0), 2.0, None, 10.0, 15.0, 90.0, None, 95.0, 1.0, 1.0, 100.0),
    ]
    
    expected_column_names = [field.name for field in expected_schema.fields]
    expected_df = self.spark.createDataFrame(expected_data, expected_column_names)

    self.assertEqual(result_df.schema, expected_df.schema)
    self.assertEqual(result_df.collect(), expected_df.collect())
    
if __name__ == '__main__':
  # unittest.main()
  unittest.main(argv=[""], exit=False)
