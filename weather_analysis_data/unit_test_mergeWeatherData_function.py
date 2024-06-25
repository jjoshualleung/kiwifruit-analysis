import unittest
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal
from merge_weather_station_data_sets import mergeWeatherData
from merged_weather_data_expected_schema import merged_weather_data_expected_schema
from pyspark.testing.utils import assertDataFrameEqual

class TestmergeWeatherData(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestmergeWeatherData").getOrCreate()

  def test_3_weather_station_tables_with_one_row_of_data(self):

    test_station1_data = [{
      "Date": "1/1/2023 00:00",
      "Rain (mm)": "1.0",
      "Max Temperature (�C)": "20.0",
      " Min Temperature(�C)": "10.0",
      "AVG Temperature(�C)": "15.0",
      "Max Humidity(%)": "90.0",
      "MinHumidity(%)": "100.0",
      "AVG Humidity(%)": "95.0",
      "SolarRad (MJ/m�)": "1.0",
      "WindSpeed(m/s)": "1.0",
      "WindGusts(m/s)": "100.0",
    }]

    test_station2_data = [{
      "Date": "2/2/2023 01:00",
      "Rain (mm)": "1.0",
      "Max Temperature (�C)": "20.0",
      " Min Temperature(�C)": "10.0",
      "AVG Temperature(�C)": "15.0",
      "Max Humidity(%)": "90.0",
      "MinHumidity(%)": "100.0",
      "AVG Humidity(%)": "95.0",
      "SolarRad (MJ/m�)": "1.0",
      "WindSpeed(m/s)": "1.0",
      "WindGusts(m/s)": "100.0",
    }]

    test_station3_data =  [{
      "Date": "3/3/2023 02:00",
      "Rain (mm)": "2.0",
      "Max Temperature (�C)": "--",
      " Min Temperature(�C)": "10.0",
      "AVG Temperature(�C)": "15.0",
      "Max Humidity(%)": "90.0",
      "MinHumidity(%)": "--",
      "AVG Humidity(%)": "95.0",
      "SolarRad (MJ/m�)": "1.0",
      "WindSpeed(m/s)": "1.0",
      "WindGusts(m/s)": "100.0",
    }]

    expected_weather_data_dict = [
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
        "datetime": datetime(2023, 2, 2, 1, 0, 0),
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
      {
        "datetime": datetime(2023, 3, 3, 2, 0, 0),
        "rain_mm": 2.0,
        "max_temperature_C": None,
        "min_temperature_C": 10.0,
        "avg_temperature_C": 15.0,
        "max_humidity_percent": 90.0,
        "min_humidity_percent": None,
        "avg_humidity_percent": 95.0,
        "solarrad_MJ_per_m2": 1.0,
        "windspeed_m_per_s": 1.0,
        "windgusts_m_per_s": 100.0,
        "station": "station3",
      }
    ]

    test_weather_data_dict = {
      "station1": spark.createDataFrame(pd.DataFrame(test_station1_data)),
      "station2": spark.createDataFrame(pd.DataFrame(test_station2_data)),
      "station3": spark.createDataFrame(pd.DataFrame(test_station3_data))
    }

    result_merged_sdf = mergeWeatherData(test_weather_data_dict)

    expected_sdf = self.spark.createDataFrame(expected_weather_data_dict, schema=merged_weather_data_expected_schema)

    assertDataFrameEqual(expected_sdf, result_merged_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)
