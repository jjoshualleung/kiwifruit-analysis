import sys
import os
sys.path.append(os.path.abspath('..'))
import unittest
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from filter_complete_time_series_weather_data import filterCompleteWeatherTimeSeries
from merged_weather_data_expected_schema import merged_weather_data_expected_schema

class TestfilterRelevantWeatherTimeSeries(unittest.TestCase):

  def setUp(self):
    self.spark = SparkSession.builder.appName("TestfilterCompleteWeatherTimeSeries").getOrCreate()
  
  def test_4_rows_weather_time_series_data_with_2_stations(self):
    testing_time_series_weather_data = [
      {
      "datetime": datetime(2023, 1, 1, 0, 0, 0),
      "rain_mm": 1.0,
      "max_temperature_C": 20.0,
      "min_temperature_C": 10.0,
      "avg_temperature_C": 15.0,
      "max_humidity_percent": 90.0,
      "min_humidity_percent": 100.0,
      "avg_humidity_percent": 95.0,
      "solarrad_MJ_per_m2": 1.0,
      "windspeed_m_per_s": 1.0,
      "windgusts_m_per_s": 100.0,
      "station": "station1",
    },
    {
      "datetime": datetime(2023, 1, 1, 0, 0, 0),
      "rain_mm": 1.0,
      "max_temperature_C": 20.0,
      "min_temperature_C": 10.0,
      "avg_temperature_C": 15.0,
      "max_humidity_percent": 90.0,
      "min_humidity_percent": 100.0,
      "avg_humidity_percent": 95.0,
      "solarrad_MJ_per_m2": 1.0,
      "windspeed_m_per_s": 1.0,
      "windgusts_m_per_s": 100.0,
      "station": "station2",
    },
    # Case: contain null values
    {
      "datetime": datetime(2023, 1, 1, 0, 0, 0),
      "rain_mm": 1.0,
      "max_temperature_C": 20.0,
      "min_temperature_C": 10.0,
      "avg_temperature_C": None,
      "max_humidity_percent": 90.0,
      "min_humidity_percent": 100.0,
      "avg_humidity_percent": None,
      "solarrad_MJ_per_m2": 1.0,
      "windspeed_m_per_s": 1.0,
      "windgusts_m_per_s": 100.0,
      "station": "station1",
    },
    # Case: duplicates row
    {
      "date": datetime(2023, 1, 1, 0, 0, 0),
      "rain_mm": 1.0,
      "max_temperature_C": 20.0,
      "min_temperature_C": 10.0,
      "avg_temperature_C": 15.0,
      "max_humidity_percent": 90.0,
      "min_humidity_percent": 100.0,
      "avg_humidity_percent": 95.0,
      "solarrad_MJ_per_m2": 1.0,
      "windspeed_m_per_s": 1.0,
      "windgusts_m_per_s": 100.0,
      "station": "station2",
    }
    ]

    expected_relevant_time_series_sdf = self.spark.createDataFrame([{"datetime": datetime(2023, 1, 1, 0, 0, 0)}])

    testing_time_series_weather_data_sdf = self.spark.createDataFrame(testing_time_series_weather_data, merged_weather_data_expected_schema)

    testing_relevant_timeseries_sdf = filterCompleteWeatherTimeSeries(testing_time_series_weather_data_sdf)

    assertDataFrameEqual(testing_relevant_timeseries_sdf, expected_relevant_time_series_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)