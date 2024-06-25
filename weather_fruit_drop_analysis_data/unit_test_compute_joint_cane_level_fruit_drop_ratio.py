import unittest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from joint_cane_level_fruit_drop_schema import joint_cane_level_fruit_drop_schema
from compute_joint_cane_level_fruit_drop_ratio import compute_joint_cane_level_fruit_drop_ratio

class TestComputeJoinCaneLevelFruitDropRatio(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestComputeJoinCaneLevelFruitDropRatio").getOrCreate()
  
  def test_4_different_fruit_assessment_date_for_the_same_KPIN_maturity_area_bay_cane_number(self):

    test_cane_level_fruit_drop_data = [{
      "fruit_drop_assessment_date": date(2023, 7, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 2
    },
    { 
      "fruit_drop_assessment_date": date(2023, 8, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 2
    },
    { 
      "fruit_drop_assessment_date": date(2023, 9, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 0
    },
    {
      "fruit_drop_assessment_date": date(2023, 10, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3
    }
    ]

    expected_cane_level_fruit_drop_data = [{
      "fruit_drop_assessment_date": date(2023, 7, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 2,
      "prev_remaining_fruit": 10,
      "remaining_fruit": 8,
      "fruit_drop_ratio": 0.2
    },
    {
      "fruit_drop_assessment_date": date(2023, 8, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 2,
      "prev_remaining_fruit": 8,
      "remaining_fruit": 6,
      "fruit_drop_ratio": 0.25
    },
    {
      "fruit_drop_assessment_date": date(2023, 9, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 0,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 6,
      "fruit_drop_ratio": 0.0
    },
    {
      "fruit_drop_assessment_date": date(2023, 10, 19),
      "grower": "A",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 3,
      "fruit_drop_ratio": 0.5
    }
    ]

    test_cane_level_fruit_drop_data_sdf = self.spark.createDataFrame(test_cane_level_fruit_drop_data,joint_cane_level_fruit_drop_schema)
    expected_cane_level_fruit_drop_data_sdf = self.spark.createDataFrame(expected_cane_level_fruit_drop_data, joint_cane_level_fruit_drop_schema)

    test_joint_cane_level_fruit_drop_sdf = compute_joint_cane_level_fruit_drop_ratio(test_cane_level_fruit_drop_data_sdf)

    assertDataFrameEqual(expected_cane_level_fruit_drop_data_sdf, test_joint_cane_level_fruit_drop_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)
