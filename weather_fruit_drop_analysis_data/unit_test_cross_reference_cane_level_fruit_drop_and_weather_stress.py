import unittest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, LongType
from pyspark.testing.utils import assertDataFrameEqual
from cumulative_weather_stress_schema import cumulative_weather_stress_schema
from joint_cane_level_fruit_drop_schema import joint_cane_level_fruit_drop_schema
from joint_fruit_drop_cumulative_weather_stress_schema import joint_fruit_drop_cumulative_weather_stress_schema
from cross_reference_cane_level_fruit_drop_and_weather_stress import cross_reference_cane_level_fruit_drop_and_weather_stress

class TestCrossReferenceWeatherStressAndFruitDrop(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName("TestCrossReferenceWeatherStressAndFruitDrop").getOrCreate()

  def test_2_rows_cane_level_fruit_drop_and_cumulative_weather_stress_sdf(self):
    
    test_cumulative_weather_stress_data = [
    {
      "date": date(2023, 6, 19),
      "station": "station1",
      "cumulative_weather_stress": 100
    },
    {
      "date": date(2023, 7, 19),
      "station": "station1",
      "cumulative_weather_stress": 200
    },
    {
      "date": date(2023, 6, 19),
      "station": "station2",
      "cumulative_weather_stress": 100
    },
    {
      "date": date(2023, 7, 19),
      "station": "station2",
      "cumulative_weather_stress": 200
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
      "fruit_drop_ratio": 0.5
    },
    {      
      "fruit_drop_assessment_date": date(2023, 7, 19),
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
      "fruit_drop_ratio": 0.5
    },
    { 
      "fruit_drop_assessment_date": date(2023, 6, 19),
      "WeatherStation": "station2",
      "grower": "B",
      "KPIN": 2000,
      "maturity_area": "AP02",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 3,
      "fruit_drop_ratio": 0.5
    },
    {
      "fruit_drop_assessment_date": date(2023, 7, 19),
      "WeatherStation": "station2",
      "grower": "B",
      "KPIN": 2000,
      "maturity_area": "AP02",
      "bay": 1,
      "cane_number": 1,
      "initial_fruit_number": 10,
      "total_dropped_dry_and_healthy_peduncle": 3,
      "prev_remaining_fruit": 6,
      "remaining_fruit": 3,
      "fruit_drop_ratio": 0.5
    }
    ]

    expected_joint_cane_level_fruit_drop_and_weather_data = [
    {
      "fruit_drop_assessment_date": date(2023, 6, 19),
      "station": "station1",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_weather_stress": 100
    },
    {
      "fruit_drop_assessment_date": date(2023, 7, 19),
      "station": "station1",
      "KPIN": 1000,
      "maturity_area": "AP01",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_weather_stress": 200
    },
    {
      "fruit_drop_assessment_date": date(2023, 6, 19),
      "station": "station2",
      "KPIN": 2000,
      "maturity_area": "AP02",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_weather_stress": 100
    },
    {
      "fruit_drop_assessment_date": date(2023, 7, 19),
      "station": "station2",
      "KPIN": 2000,
      "maturity_area": "AP02",
      "bay": 1,
      "cane_number": 1,
      "fruit_drop_ratio": 0.5,
      "cumulative_weather_stress": 200
    }
    ]
    
    test_cumulative_weather_stress_sdf = self.spark.createDataFrame(test_cumulative_weather_stress_data, cumulative_weather_stress_schema)
    

    test_cane_level_fruit_drop_sdf = self.spark.createDataFrame(test_joint_cane_level_fruit_drop_data, joint_cane_level_fruit_drop_schema)
    

    expected_joint_fruit_drop_cumulative_weather_stress_sdf = self.spark.createDataFrame(expected_joint_cane_level_fruit_drop_and_weather_data, joint_fruit_drop_cumulative_weather_stress_schema)
    

    joint_fruit_drop_cumulative_weather_stress_sdf = cross_reference_cane_level_fruit_drop_and_weather_stress(test_cane_level_fruit_drop_sdf, test_cumulative_weather_stress_sdf)
    
    assertDataFrameEqual(joint_fruit_drop_cumulative_weather_stress_sdf, expected_joint_fruit_drop_cumulative_weather_stress_sdf)

if __name__ == '__main__':
  unittest.main(argv=[""], exit=False)