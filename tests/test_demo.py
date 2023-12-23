"""
This module is for unit test.
Author: Ei Yamaguchi
Date: 2023-12-23
"""
import sys
sys.dont_write_bytecode = True

import warnings
warnings.simplefilter('ignore')

import pytest
import pandas as pd
import pandera as pa
import pyspark.pandas as ps
from pandera.typing import Series
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, LongType

from demo.demo import increase_date, fetch_date, pandas_to_pyspark, validate_data

spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(
        column_name, regexp_replace(col(column_name), "\\s+", " ")
    )

    return df_transformed


def test_single_space(spark_fixture):
    """
    Testing PySpark Demo
    https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html
    """
    sample_data = [
        {"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
        {"name": "Bob  T.", "age": 35},
        {"name": "Eve   A.", "age": 28},
    ]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28},
    ]

    expected_df = spark.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)


def test_increase_date():
    current_day = "20231101"

    ret = increase_date(current_day, 1)

    assert ret == 20231102


def test_fetch_data():
    transformed_df = fetch_date("10001", "20221206")

    expected_data = [
        {"ten_id": "10001", "day": "20221201", "count": 10},
        {"ten_id": "10002", "day": "20221202", "count": 20},
        {"ten_id": "10003", "day": "20221203", "count": 30},
        {"ten_id": "10004", "day": "20221204", "count": 40},
        {"ten_id": "10005", "day": "20221205", "count": 50},
        {"ten_id": "10006", "day": "20221206", "count": 60},
    ]

    schema = StructType(
        [
            StructField("ten_id", StringType(), True),
            StructField("day", StringType(), True),
            StructField("count", LongType(), True),
        ]
    )

    expected_df = spark.createDataFrame(expected_data, schema=schema)
    transformed_df = transformed_df.to_spark()

    assertDataFrameEqual(transformed_df, expected_df)


def test_pandas_to_pyspark():
    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)
    psd = pandas_to_pyspark(df)

    assert ps.frame.DataFrame == type(psd)


def test_validate_data():
    
    class Pandas_Schema(pa.SchemaModel):
        name: Series[str]
        age: Series[int] = pa.Field(in_range={"min_value": 18, "max_value": 60})
        
    # テスト用のデータ
    test_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [20, 21, 22]})

    test_data_spark = pandas_to_pyspark(test_data)

    # 検証
    validated_data = validate_data(test_data_spark, Pandas_Schema)

    assert validated_data is not None
    assert validated_data["name"].count() == 3


def test_validate_data_with_invalid_data():
    
    class Pandas_Schema(pa.SchemaModel):
        name: Series[str]
        age: Series[int] = pa.Field(in_range={"min_value": 18, "max_value": 60})
        
    # テスト用のデータ
    test_data = pd.DataFrame(
        {"name": ["Alice", "Bob", "Charlie"], "age": [20, 80, 22]}
    )

    test_data_spark = pandas_to_pyspark(test_data)

    # 検証
    validated_data = validate_data(test_data_spark, Pandas_Schema)

    assert validated_data is not None
    assert validated_data["name"].count() == 2
