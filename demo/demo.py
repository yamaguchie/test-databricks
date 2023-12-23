"""
This module is used for demo purposes.
Author: Ei Yamaguchi
Date: 2023-06-20
"""
from utils.utils import Logger, Params, Pandas_Schema

import pandas as pd
import pandera as pa
import pyspark.pandas as ps
import warnings

from typing import Union
from omegaconf import OmegaConf
from datetime import datetime, timezone, timedelta


warnings.filterwarnings("ignore")


def increase_date(current_day: str, split_day: int) -> int:
    """Increase date by split day

    Args:
        current_day: date.
        split_day: the number of days that need to be increased

    Returns:
        Date after increase.
    """
    Logger().logger.info("increase data", method="increase_date")
    calculate_date = datetime.strptime(current_day, "%Y%m%d")
    increase_date = int(calculate_date.strftime("%Y%m%d")) + split_day

    return increase_date


def fetch_date(ten_id: str, day: str) -> ps.frame.DataFrame:
    """Get the input data.

    Args:
        ten_id: 10001.
        day: Day after increase.

    Returns:
        Input data of type pyspark.
    """
    Logger().logger.info("fetch data", method="fetch_date")
    df_input_list_pd = pd.DataFrame(
        {
            "ten_id": [ten_id, "10002", "10003", "10004", "10005", "10006"],
            "day": [
                "20221201",
                "20221202",
                "20221203",
                "20221204",
                "20221205",
                day,
            ],
            "count": [10, 20, 30, 40, 50, 60],
        }
    )
    df_input_list_ps = pandas_to_pyspark(df_input_list_pd)
    return df_input_list_ps


def pandas_to_pyspark(pandas_data: pd.DataFrame) -> ps.frame.DataFrame:
    """pandasをpysparkに変換.

    Args:
        pandas_data: Pandas type data.

    Returns:
        pyspark type data after convert.
    """
    Logger().logger.info("pandas to pyspark")
    values = pandas_data.values.tolist()
    columns = pandas_data.columns.tolist()
    pyspark_data = ps.DataFrame(values, columns=columns)
    return pyspark_data


def validate_data(
    data: Union[ps.frame.DataFrame, pd.DataFrame],
    schema: Pandas_Schema, debug=True
) -> Union[ps.frame.DataFrame, pd.DataFrame]:
    """データをvalidate.

    Args:
        data: pandas or pyspark.
        schema: Pandas_Schema

    Returns:
        Verified data.
    """
    Logger().logger.info("validate data")
    try:
        if type(data) == ps.frame.DataFrame:
            data = schema.validate(
                data.to_pandas(), lazy=debug, inplace=True
            )  # 定義されたスキーマに対してデータを検証して
            data = pandas_to_pyspark(data)
        elif type(data) == pd.core.frame.DataFrame:
            data = schema.validate(
                data, lazy=debug, inplace=True
            )  # 定義されたスキーマに対してデータを検証して
        else:
            raise TypeError("Data Type Error")
    except pa.errors.SchemaErrors as SchemaErrors:
        Logger().logger.warning(
            "SchemaErrors: some data did not pass schema validation"
        )
        drop_index = SchemaErrors.failure_cases  # 検証されていないデータのインデックスを取得して
        Logger().logger.warning("the index of the data need to delete")
        print(drop_index)
        data = drop_data(data, drop_index)
        Logger().logger.warning("dataset after deleting unverified data")
        print(data)
    return data


def drop_data(
    data: Union[ps.frame.DataFrame, pd.DataFrame], drop_index: pd.DataFrame
) -> pd.DataFrame:
    """検証に失敗したデータの削除.

    Args:
        data: pandas or pyspark.
        drop_index (list): Index for the data to be deleted.

    Returns:
        Data (Deleted unverified).
    """
    Logger().logger.info("drop data")
    if type(data) == ps.frame.DataFrame:
        data_pd = data.to_pandas()
        data_pd = data_pd.drop(
            data_pd.index[sorted(drop_index["index"].tolist())]
        )  # 検証されていないデータをデータセットから削除して
        data = pandas_to_pyspark(data_pd)
    elif type(data) == pd.core.frame.DataFrame:
        data = data.drop(
            data.index[sorted(drop_index["index"].tolist())]
        )  # 検証されていないデータをデータセットから削除して
    else:
        raise Exception("Drop Data Error")
    return data


def main():
    """処理制御（main関数）

    ・データ取得１処理呼び出し
    ・UDF呼び出し、データ統合
    ・データ保存
    """
    # パラメータの取得
    params = OmegaConf.structured(Params)
    ten_id = params.ten_id
    
    

    # 日付変数設定
    current_day = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    increase_day = str(increase_date(current_day, 1))

    df_data = fetch_date(ten_id, increase_day)  # データ取得処理呼び出し

    df_data = validate_data(df_data, Pandas_Schema)  # データをcheck処理呼び出し

    return


if __name__ == "__main__":
    main()

