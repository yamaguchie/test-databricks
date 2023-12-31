import pyspark.pandas as ps
import pandas as pd


def hoge():
    data = {
        "name": ["Alice", "Bob", "Charlie", "David", "Ella"],
        "age": [25, 30, 18, 42, 33],
        "country": ["USA", "Canada", "UK", "USA", "Australia"],
    }
    df = pd.DataFrame(data)
    sdf = ps.from_pandas(df)

    ps.sql("use catalog catalog_kadokura")
    ps.sql("use database default")

    sdf.to_table("catalog_kadokura.default.zzz", format="delta", mode="append")

    tmp = ps.read_table("catalog_kadokura.default.zzz")

    tmp.to_table("z300")