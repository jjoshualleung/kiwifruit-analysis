import os
import sys
import unittest
sys.path.append(os.path.abspath('..'))
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from compute_cumulative_weather_stress import compute_cumulative_weather_stress
from cumulative_weather_stress_schema import cumulative_weather_stress_schema
from weather_analysis_data.merged_weather_data_expected_schema import merged_weather_data_expected_schema

class TestComputeCumulativeWeatherStress(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestComputeCumulativeWeatherStress").getOrCreate()

  def test_4_timeseries_from_merge_weather_data_sdf(self):

    test_complete_hourly_timeseries_weather_data = [
    {
      "datetime": datetime(2023, 1, 1, 0, 0, 0),
      "rain_mm": 0.5,
      "max_temperature_C": 35.0,
      "min_temperature_C": 30.0,
      "avg_temperature_C": 32.0,
      "max_humidity_percent": 100.0,
      "min_humidity_percent": 70.0,
      "avg_humidity_percent": 85.0,
      "solarrad_MJ_per_m2": 0.5,
      "windspeed_m_per_s": 1.5,
      "windgusts_m_per_s": 2.0,
      "station": "station1"
    },
    {
      "datetime": datetime(2023, 1, 1, 2, 0, 0),
      "rain_mm": 0.5,
      "max_temperature_C": 35.0,
      "min_temperature_C": 30.0,
      "avg_temperature_C": 32.0,
      "max_humidity_percent": 100.0,
      "min_humidity_percent": 70.0,
      "avg_humidity_percent": 85.0,
      "solarrad_MJ_per_m2": 0.5,
      "windspeed_m_per_s": 1.5,
      "windgusts_m_per_s": 2.0,
      "station": "station1"
    },
    {
      "datetime": datetime(2023, 1, 2, 1, 0, 0),
      "rain_mm": 0.5,
      "max_temperature_C": 35.0,
      "min_temperature_C": 30.0,
      "avg_temperature_C": 32.0,
      "max_humidity_percent": 100.0,
      "min_humidity_percent": 70.0,
      "avg_humidity_percent": 85.0,
      "solarrad_MJ_per_m2": 0.5,
      "windspeed_m_per_s": 1.5,
      "windgusts_m_per_s": 2.0,
      "station": "station1"
    },
    {
      "datetime": datetime(2023, 1, 2, 2, 0, 0),
      "rain_mm": 0.5,
      "max_temperature_C": 35.0,
      "min_temperature_C": 30.0,
      "avg_temperature_C": 32.0,
      "max_humidity_percent": 100.0,
      "min_humidity_percent": 70.0,
      "avg_humidity_percent": 85.0,
      "solarrad_MJ_per_m2": 0.5,
      "windspeed_m_per_s": 1.5,
      "windgusts_m_per_s": 2.0,
      "station": "station1"
    }
    ]

    expected_cumulative_weather_stress_data = [
    {
      "date": date(2023, 1, 1),
      "station": "station1",
      "cumulative_weather_stress": 1
    },
    {
      "date": date(2023, 1, 1),
      "station": "station1",
      "cumulative_weather_stress": 2
    },
    {
      "date": date(2023, 1, 2),
      "station": "station1",
      "cumulative_weather_stress": 3
    },
    {
      "date": date(2023, 1, 2),
      "station": "station1",
      "cumulative_weather_stress": 4
    }
    ]
    
    test_complete_hourly_timeseries_weather_sdf = self.spark.createDataFrame(test_complete_hourly_timeseries_weather_data, merged_weather_data_expected_schema)
    expected_cumulative_weather_stress_sdf = self.spark.createDataFrame(expected_cumulative_weather_stress_data, cumulative_weather_stress_schema)

    test_cumulative_weather_stress_sdf = compute_cumulative_weather_stress(test_complete_hourly_timeseries_weather_sdf)
    
    assertDataFrameEqual(expected_cumulative_weather_stress_sdf, test_cumulative_weather_stress_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)