# title: IEX_ETL_SSERFF_DATA_RATING_TABLE_RATE_TABLE.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date:5/18/2021
# last updated date: github will have latest date

# desc: ETL Tool to extract data from excel files and insert data into Teradata

# from Teradata_Connect import td_conn
import teradataml
import pandas as pd
import pandas_settings as pandas_settings
import os
import variables as variables
import datetime
import getpass

pandas_settings.pd_set_option()

start_time = datetime.datetime.now()
print(f'Start time: {start_time}')

user = getpass.getuser()
print(f'{user} is the current user logged into IEX_ETL.py')

# Source Data Parameters
source = 'SERFF'
file_name = r'DATA_BENEFITS'
excel_sheet_names = ['Cost Share Variances',  'Cost',  'Share',  'Variances']
excel_sheet_names_2 = ['Benefits Package',  'Benefits',  'Package']
sheet_name = 'COST_SHARE_VARIANCES'
table_name = f'{source}_{file_name}_{sheet_name}'
td_table_name_stg = f'{table_name}_STG'
td_table_name_t = f'{table_name}_T'
file_list = []  # list of files
file_list_cnt = []  # list of files

# Landing database
if_exists = 'append'

path = variables.windows_test_file_path

#path = r'C:\Users\tshiffl1\OneDrive - UHG\IEX_DB\Test\Test\2022_DATA_BENEFITS.xlsx'

col_data = pd.DataFrame()

col_data = pd.read_excel(path, sheet_name='Cost Share Variances 1', header=None, nrows=3) #, usecols=num_list)

col_data = col_data.fillna(method='pad', axis=1)

col_data = col_data.replace('All fields with an asterisk (*) are required', '_')
col_data = col_data.replace(['\(', '/'], ' ', regex=True)
col_data = col_data.replace(['\*', r'\n','\)', '\+', '\?',','], '', regex=True)
col_data = col_data.replace('&', 'and', regex=True)
col_data = col_data.replace(' ', '_', regex=True)
col_data = col_data.replace('__', '_', regex=True)

cols = col_data.iloc[0] + '_' + col_data.iloc[1] + '_' + col_data.iloc[2]

cols = cols.replace('__', '', regex=True)

cols = cols.str.lower()

col_data.columns = cols

col_data = col_data.loc[:, ~col_data.columns.duplicated()]

total_cols = len(col_data.columns)

found = col_data.isin(['Copay']).any()
column_name = found[found].index.values[0]
integer_index = col_data.columns.get_loc(column_name)

col_range_list = []

for x in range(integer_index, total_cols + 1, 6):
    col_range_list.append(x)

print(col_range_list)
len_col_range_list = len(col_range_list)

all_data = pd.DataFrame()

for i in range(0, len_col_range_list):
    x = i
    y = i + 1
    num_range = range(col_range_list[x], col_range_list[y])

    num_list = list(num_range)

    num_list.insert(0, 0)

    excel_read_df = pd.read_excel(path, sheet_name='Cost Share Variances 1', header=2, usecols=num_list)

    cols = col_data.iloc[:, num_list]

    excel_read_df.columns = cols.columns

    benefit_name = cols.iloc[0, 1]

    id_vars = cols.columns[0]

    value_vars = cols.columns[1]

    excel_read_df_melt = pd.melt(excel_read_df,
                                 id_vars=id_vars,
                                 var_name='benefit',
                                 value_vars=cols.columns[1],
                                 value_name='copay_in_network_tier_1'
                                 )

    excel_read_df_melt.insert(2, 'benefit_name', benefit_name)
    excel_read_df_melt = excel_read_df_melt.drop(columns='benefit', axis=1)

    excel_read_df_melt_2 = pd.melt(excel_read_df,
                                   id_vars=id_vars,
                                   var_name='benefit',
                                   value_vars=cols.columns[2],
                                   value_name='copay_in_network_tier_2')

    excel_read_df_melt_2.insert(2, 'benefit_name', benefit_name)
    excel_read_df_melt_2 = excel_read_df_melt_2.drop(columns='benefit', axis=1)

    excel_read_df_melt_3 = pd.melt(excel_read_df,
                                   id_vars=id_vars,
                                   var_name='benefit',
                                   value_vars=cols.columns[3],
                                   value_name='copay_out_of_network')

    excel_read_df_melt_3.insert(2, 'benefit_name', benefit_name)
    excel_read_df_melt_3 = excel_read_df_melt_3.drop(columns='benefit', axis=1)

    excel_read_df_melt_4 = pd.melt(excel_read_df,
                                   id_vars=id_vars,
                                   var_name='benefit',
                                   value_vars=cols.columns[4],
                                   value_name='coinsurance_in_network_tier_1')

    excel_read_df_melt_4.insert(2, 'benefit_name', benefit_name)
    excel_read_df_melt_4 = excel_read_df_melt_4.drop(columns='benefit', axis=1)

    excel_read_df_melt_5 = pd.melt(excel_read_df,
                                   id_vars=id_vars,
                                   var_name='benefit',
                                   value_vars=cols.columns[5],
                                   value_name='coinsurance_in_network_tier_2')

    excel_read_df_melt_5.insert(2, 'benefit_name', benefit_name)
    excel_read_df_melt_5 = excel_read_df_melt_5.drop(columns='benefit', axis=1)

    excel_read_df_melt_6 = pd.melt(excel_read_df,
                                   id_vars=id_vars,
                                   var_name='benefit',
                                   value_vars=cols.columns[6],
                                   value_name='coinsurance_out_of_network')

    excel_read_df_melt_6.insert(2, 'benefit_name', benefit_name)
    excel_read_df_melt_6 = excel_read_df_melt_6.drop(columns='benefit', axis=1)

    excel_read_df_melt_merge = pd.merge(excel_read_df_melt, excel_read_df_melt_2, on=[id_vars, 'benefit_name'])
    excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_3, on=[id_vars, 'benefit_name'])
    excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_4, on=[id_vars, 'benefit_name'])
    excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_5, on=[id_vars, 'benefit_name'])
    excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_6, on=[id_vars, 'benefit_name'])

    all_data = all_data.append(excel_read_df_melt_merge, ignore_index=True)

    del excel_read_df_melt_merge

    if y == (len_col_range_list - 1):
        break

all_data.to_csv(f'csv/SERFF_DATA_BENEFITS_COST_SHARE_VARIANCES_PLAN_VARIANT_BENEFITS.csv')
print(all_data)
