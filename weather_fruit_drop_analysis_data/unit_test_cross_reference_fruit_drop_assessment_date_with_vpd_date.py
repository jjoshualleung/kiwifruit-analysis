import unittest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from joint_cane_level_fruit_drop_schema import joint_cane_level_fruit_drop_schema
from joint_fruit_drop_cumulative_vpd_schema import joint_fruit_drop_cumulative_vpd_schema
from cumulative_vapour_pressure_deficit_stress_schema import cumulative_vapour_pressure_deficit_stress_schema
from cross_reference_cane_level_fruit_drop_and_vpd_stress import cross_reference_cane_level_fruit_drop_and_vpd_stress


class TestCrossReferenceFruitDropAssessmentDateWithVPDDate(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestCrossReferenceFruitDropAssessmentDateWithVPDDate").getOrCreate()

  def test_row_matching_if_max_cumulative_vpd_stress_date_is_less_than_or_equal_to_fruit_drop_assessment_date(self):

    test_cumulative_vpd_stress_data = [
    {
      "date": date(2023, 6, 19),
      "station": "station1",
      "cumulative_vapour_pressure_deficit_stress": 10
    },
    {
      "date": date(2023, 7, 19),
      "station": "station1",
      "cumulative_vapour_pressure_deficit_stress": 20
    },
    { 
      "date": date(2023, 8, 19),
      "station": "station1",
      "cumulative_vapour_pressure_deficit_stress": 30
    }
    ]

    test_joint_cane_level_fruit_drop_data = [
    {
      "fruit_drop_assessment_date": date(2023, 6, 19),
      "WeatherStation": "station1",
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 3,
      "fruit_drop_ratio": 0.5,
      "cumulative_fruit_drop_ratio": 0.5
    },
    {      
      "fruit_drop_assessment_date": date(2023, 7, 30),
      "WeatherStation": "station1",
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 3,
      "fruit_drop_ratio": 0.5,
      "cumulative_fruit_drop_ratio": 0.5
    },
    {
      "fruit_drop_assessment_date": date(2023, 8, 10),
      "WeatherStation": "station1",
      "grower": "1",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 3,
      "fruit_drop_ratio": 0.5,
      "cumulative_fruit_drop_ratio": 0.5
    }
    ]

    expected_joint_cane_level_fruit_drop_and_vpd_data = [
      { 
      "fruit_drop_assessment_date": date(2023, 6, 19),
      "station": "station1",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_fruit_drop_ratio": 0.5,
      "cumulative_vapour_pressure_deficit_stress": 10
    },
    {
      "fruit_drop_assessment_date": date(2023, 7, 30),
      "station": "station1",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_fruit_drop_ratio": 0.5,
      "cumulative_vapour_pressure_deficit_stress": 20
    },
    {
      "fruit_drop_assessment_date": date(2023, 8, 10),
      "station": "station1",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_fruit_drop_ratio": 0.5,
      "cumulative_vapour_pressure_deficit_stress": 20
    }
    ]
    
    test_cumulative_vpd_stress_sdf = self.spark.createDataFrame(test_cumulative_vpd_stress_data, cumulative_vapour_pressure_deficit_stress_schema)

    test_cane_level_fruit_drop_sdf = self.spark.createDataFrame(test_joint_cane_level_fruit_drop_data, joint_cane_level_fruit_drop_schema)
    
    expected_joint_fruit_drop_cumulative_vpd_stress_sdf = self.spark.createDataFrame(expected_joint_cane_level_fruit_drop_and_vpd_data, joint_fruit_drop_cumulative_vpd_schema)
    
    joint_fruit_drop_cumulative_vpd_stress_sdf = cross_reference_cane_level_fruit_drop_and_vpd_stress(test_cane_level_fruit_drop_sdf, test_cumulative_vpd_stress_sdf)
    
    assertDataFrameEqual(joint_fruit_drop_cumulative_vpd_stress_sdf, expected_joint_fruit_drop_cumulative_vpd_stress_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)