# Databricks notebook source

# MAGIC %load_ext autoreload

# COMMAND ----------

# MAGIC %autoreload 2

# COMMAND ----------

import pytest
import os

import sys
sys.dont_write_bytecode = True

import warnings
warnings.simplefilter('ignore')


test_path = "./tests"

pytest_pre_args = []
pytest_discovery_args = [
    "-v",
    "-rsx",
    "--showlocals",
    "--tb=short",
    "-s",
    "--pyargs",
    "-p no:warnings ",
    test_path,
]
pytest_pre_args.extend(pytest_discovery_args)
ut_result = pytest.main(pytest_pre_args)