import pyspark
import pandas as pd
import os
import subprocess
from delta import *


# from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

import pyspark.pandas as ps

data = {'name': ['Alice', 'Bob', 'Charlie', 'David', 'Ella'],
        'age': [25, 30, 18, 42, 33],
        'country': ['USA', 'Canada', 'UK', 'USA', 'Australia']}
df = pd.DataFrame(data)
sdf = ps.from_pandas(df)
# breakpoint()
# import pdb; pdb.set_trace() 
# sdf.to_table("aaaa",mode="append")
sdf.to_table("zzz",format="delta",mode="append")
