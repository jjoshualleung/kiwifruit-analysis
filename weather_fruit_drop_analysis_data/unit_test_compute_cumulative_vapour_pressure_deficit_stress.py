import os
import sys
import unittest
sys.path.append(os.path.abspath('..'))
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from weather_analysis_data.vapour_pressure_deficit_schema import vapour_pressure_deficit_schema
from cumulative_vapour_pressure_deficit_stress_schema import cumulative_vapour_pressure_deficit_stress_schema
from compute_cumulative_vapour_pressure_deficit_stress import compute_cumulative_vapour_pressure_deficit_stress

class TestComputeCumulativeVapourPressureDeficitStress(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestComputeCumulativeVapourPressureDeficitStress").getOrCreate()

  def test_3_records_from_vpd_weather_data_sdf(self):

    test_vapour_pressure_deficit_weather_data = [
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
    },
    {
    "date": date(2023, 1, 2),
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
    },
    {
    "date": date(2023, 1, 3),
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

    expected_cumulative_vpd_data = [
    {
      "date": date(2023, 1, 1),
      "station": "station1",
      "cumulative_vapour_pressure_deficit_stress": 1
    },
    {
      "date": date(2023, 1, 2),
      "station": "station1",
      "cumulative_vapour_pressure_deficit_stress": 2
    },
    {
      "date": date(2023, 1, 3),
      "station": "station1",
      "cumulative_vapour_pressure_deficit_stress": 3
    }
    ]
    
    test_vpd_data_sdf = self.spark.createDataFrame(test_vapour_pressure_deficit_weather_data, vapour_pressure_deficit_schema)
    expected_cumulative_vpd_stress_sdf = self.spark.createDataFrame(expected_cumulative_vpd_data, cumulative_vapour_pressure_deficit_stress_schema)

    test_cumulative_vpd_stress_sdf = compute_cumulative_vapour_pressure_deficit_stress(test_vpd_data_sdf)
    
    assertDataFrameEqual(expected_cumulative_vpd_stress_sdf, test_cumulative_vpd_stress_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)