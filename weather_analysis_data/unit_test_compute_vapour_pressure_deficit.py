import unittest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from compute_vapour_pressure_deficit import compute_vapour_pressure_deficit
from merged_weather_data_expected_schema import merged_weather_data_expected_schema
from vapour_pressure_deficit_schema import vapour_pressure_deficit_schema

class TestComputeVapourPressureDeficit(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestComputeVapourPressureDeficit").getOrCreate()

  def test_2_records_for_computing_daily_vpd(self):
    test_weather_data = [
    {
    "datetime": datetime(2023, 1, 1, 0, 0, 0),
    "rain_mm": 1.0,
    "max_temperature_C": 25.0,
    "min_temperature_C": 10.0,
    "avg_temperature_C": 18.0,
    "max_humidity_percent": 100.0,
    "min_humidity_percent": 54.0,
    "avg_humidity_percent": 82.0,
    "solarrad_MJ_per_m2": 1.0,
    "windspeed_m_per_s": 1.0,
    "windgusts_m_per_s": 100.0,
    "station": "station1",
    },
    {
    "datetime": datetime(2023, 1, 1, 1, 0, 0),
    "rain_mm": 1.0,
    "max_temperature_C": 30.0,
    "min_temperature_C": 20.0,
    "avg_temperature_C": 25.0,
    "max_humidity_percent": 82.0,
    "min_humidity_percent": 32.0,
    "avg_humidity_percent": 54.0,
    "solarrad_MJ_per_m2": 1.0,
    "windspeed_m_per_s": 1.0,
    "windgusts_m_per_s": 100.0,
    "station": "station1",
    }
    ]

    expected_weather_data_with_vpd = [
    {
    "date": date(2023, 1, 1),
    "daily_maximum_avg_temperature_C": 25.0,
    "daily_minimum_avg_temperature_C": 18.0,
    "daily_maximum_avg_humidity_percent": 82.0,
    "daily_minimum_avg_humidity_percent": 54.0,
    "station": "station1",
    "e_t_max": 3.16777,
    "e_t_min": 2.06400,
    "es": 2.61588,
    "rh_max": 82.0,
    "rh_min": 54.0,
    "ea": 1.70153,
    "vpd": 0.91435
    }
    ]

    test_weather_data_sdf = self.spark.createDataFrame(test_weather_data, merged_weather_data_expected_schema)
    
    expected_weather_data_with_vpd_sdf = self.spark.createDataFrame(expected_weather_data_with_vpd, vapour_pressure_deficit_schema)

    test_cumulative_weather_stress_sdf = compute_vapour_pressure_deficit(test_weather_data_sdf, 1)
    
    assertDataFrameEqual(expected_weather_data_with_vpd_sdf, test_cumulative_weather_stress_sdf, rtol=1e-4)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)
