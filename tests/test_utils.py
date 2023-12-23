"""
This module is for unit test.
Author: Ei Yamaguchi
Date: 2023-12-23
"""

import sys
sys.dont_write_bytecode = True

import warnings
warnings.simplefilter('ignore')

from omegaconf import OmegaConf
from utils.utils import Params, Pandas_Schema, Logger
from pandera.typing import DataFrame
import pytest

def test_ten_id():
    """Used for Unit Test.

    Verify if the values obtained after structuring
    are equal to the initial values.
    """
    assert OmegaConf.structured(Params).ten_id == "10001"


def test_Pandas_Schema_ok():
    """Used for Unit Test.

    test Pandas Schema class
    """
    df = DataFrame[Pandas_Schema](
        {
        "ten_id": ["10001", "10002", "10003"],
        "count": [30, 60, 20],
        "day": ["200", "156", "365"],
        }
    )

    assert df is not None


def test_Pandas_Schema_ng():
    """Used for Unit Test.

    test Pandas Schema class
    """
    with pytest.raises(Exception) as errinfo:
        print("#" * 20)
        df = DataFrame[Pandas_Schema](
            {
            "ten_id": ["10001", "10002", "10003"],
            "count": [30, 60, 120],
            "day": ["200", "156", "365"],
            }
        )
        print("#" * 20)
        
    print(errinfo.value)

    assert "Check in_range: in_range(20, 100)" in str(errinfo.value)
