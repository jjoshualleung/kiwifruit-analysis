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

  def test_3_records_for_computing_vpd_with_relative_humdity_in_periods_of_3_days(self):
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
    "datetime": datetime(2023, 1, 2, 0, 0, 0),
    "rain_mm": 1.0,
    "max_temperature_C": 25.0,
    "min_temperature_C": 10.0,
    "avg_temperature_C": 18.0,
    "max_humidity_percent": 100.0,
    "min_humidity_percent": 54.0,
    "avg_humidity_percent": 70.0,
    "solarrad_MJ_per_m2": 1.0,
    "windspeed_m_per_s": 1.0,
    "windgusts_m_per_s": 100.0,
    "station": "station1",
    },
    {
    "datetime": datetime(2023, 1, 3, 0, 0, 0),
    "rain_mm": 1.0,
    "max_temperature_C": 25.0,
    "min_temperature_C": 10.0,
    "avg_temperature_C": 18.0,
    "max_humidity_percent": 82.0,
    "min_humidity_percent": 30.0,
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
    "daily_maximum_avg_temperature_C": 18.0,
    "daily_minimum_avg_temperature_C": 18.0,
    "daily_maximum_avg_humidity_percent": 82.0,
    "daily_minimum_avg_humidity_percent": 82.0,
    "station": "station1",
    "e_t_max": 2.0639,
    "e_t_min": 2.0639,
    "es": 2.0639,
    "rh_max": 82.0,
    "rh_min": 82.0,
    "ea": 1.6924,
    "vpd": 0.3715
    },
    {
    "date": date(2023, 1, 2),
    "daily_maximum_avg_temperature_C": 18.0,
    "daily_minimum_avg_temperature_C": 18.0,
    "daily_maximum_avg_humidity_percent": 70.0,
    "daily_minimum_avg_humidity_percent": 70.0,
    "station": "station1",
    "e_t_max": 2.0639,
    "e_t_min": 2.0639,
    "es": 2.0639,
    "rh_max": 76.0,
    "rh_min": 76.0,
    "ea": 1.5686,
    "vpd": 0.4953
    },
    {
    "date": date(2023, 1, 3),
    "daily_maximum_avg_temperature_C": 18.0,
    "daily_minimum_avg_temperature_C": 18.0,
    "daily_maximum_avg_humidity_percent": 54.0,
    "daily_minimum_avg_humidity_percent": 54.0,
    "station": "station1",
    "e_t_max": 2.0639,
    "e_t_min": 2.0639,
    "es": 2.0639,
    "rh_max": 68.666,
    "rh_min": 68.666,
    "ea": 1.4172,
    "vpd": 0.6467
    }
    ]

    test_weather_data_sdf = self.spark.createDataFrame(test_weather_data, merged_weather_data_expected_schema)
    
    expected_weather_data_with_vpd_sdf = self.spark.createDataFrame(expected_weather_data_with_vpd, vapour_pressure_deficit_schema)
    
    test_cumulative_weather_stress_sdf = compute_vapour_pressure_deficit(test_weather_data_sdf, 3)
    
    assertDataFrameEqual(expected_weather_data_with_vpd_sdf, test_cumulative_weather_stress_sdf, rtol=1e-3)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)
