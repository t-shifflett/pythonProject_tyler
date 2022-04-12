# title: pandas_settings.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date: 05/10/2021
# last updated date: github will have latest date

# desc: contain all pandas set_option settings for project.

import pandas as pd


def pd_set_option():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
