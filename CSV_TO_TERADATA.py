# title: CSV_TO_TERADATA.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date: 7/14/2021
# last updated date: github will have latest date

# desc: Tool to extract data from CSV files and insert data into Teradata

from Teradata_Connect import td_conn
import teradataml
import pandas as pd
import pandas_settings as pandas_settings
import os
import variables as variables
import datetime
import getpass
import pymssql
import
pymssql.

pandas_settings.pd_set_option()

start_time = datetime.datetime.now()
print(f'Start time: {start_time}')

user = getpass.getuser()
print(f'{user} is the current user logged into IEX_ETL.py')

# Source Data Parameters

table_name = f'SERFF_DATA_BENEFITS_COST_SHARE_VARIANCES_PLAN_VARIANT_BENEFITS'
td_table_name_stg = f'{table_name}_STG'
td_table_name_t = f'{table_name}_T'
file_list = []  # list of files
file_list_cnt = []  # list of files

# Landing database
if_exists = 'append'

path = r'C:\Users\tshiffl1\OneDrive - UHG\IEX_DB\csv_to_teradata'

file_processing_start_time = datetime.datetime.now()
print(f'File processing start Time: {file_processing_start_time}')

for root, directories, files in os.walk(path, topdown=False):
    for name in files:
        if name.endswith('.csv'):
            root_name = (os.path.join(root, name))
            file_list_cnt.append(root_name)

print(str(len(file_list_cnt)) + ' files found')

file_name_df = pd.DataFrame(file_list, columns=['Root_File_Name'])

# data to csv
file_name_df.to_csv(f'csv/{table_name}_file_list.csv')

for root, directories, files in os.walk(path, topdown=False):
    for name in files:
        if name.endswith('.csv'):
            root_name = (os.path.join(root, name))

            all_data_append = pd.DataFrame()

            all_data_append = pd.read_csv(root_name)

            file_processing_end_time = datetime.datetime.now()

            file_processing_duration = file_processing_end_time - file_processing_start_time
            print('File processing duration HH:MM:SS.MS: ' + str(file_processing_duration))

            df_file_list_cnt = len(file_name_df.index)
            print(f'{df_file_list_cnt} files processed')

            all_data_append = all_data_append.astype(str)  # change all data to str to load into teradata to prevent load errors with different

            all_data_append.drop(all_data_append.columns[all_data_append.columns.str.contains('unnamed', case=False, na=False)], axis=1, inplace=True)

            all_data_append = all_data_append.replace(['nan', 'NaN'], [None, None])

            print(all_data_append.info())
            print(all_data_append)

            df_cnt = len(all_data_append.index)
            print(f'DataFrame count: {df_cnt}')

            database_start_time = datetime.datetime.now()
            print(f'Database start Time: {database_start_time}')

            truncate_stg = f'DELETE FROM {td_table_name_stg};'
            td_conn.execute(truncate_stg)

            # if df_cnt < 100000:
            teradataml.copy_to_sql(df=all_data_append, table_name=td_table_name_stg, if_exists=if_exists,  index=True,
                                       index_label='row_id')
                # use copy_to_sql if rowcount is less than 100k
            # else:
            #     teradataml.fastload(df=all_data_append, table_name=td_table_name_stg, if_exists=if_exists, index=True,
            #                         index_label='row_id')
            #     # use copy_to_sql if rowcount is over 100k https://docs.teradata.com/r/GsM0pYRZl5Plqjdf9ixmdA/pvQq0OGIULWr5dtBfDreUg

            load_msg = f'{td_table_name_stg} has been loaded with {df_cnt} records'
            print(load_msg)

            truncate_t = f'DELETE FROM {td_table_name_t};'
            td_conn.execute(truncate_t)

            insert_into_t = f'INSERT INTO {td_table_name_t} SELECT * FROM {td_table_name_stg};'
            td_conn.execute(insert_into_t)

            td_conn.execute(truncate_stg)

            count_t = f'select count(*) from {td_table_name_t}'
            result_t = td_conn.execute(count_t)

            for row in result_t:
                print(f'{td_table_name_t} has been loaded with {row} records')

            database_end_time = datetime.datetime.now()

            database_duration = database_end_time - database_start_time
            print('Database duration HH:MM:SS.MS: ' + str(database_duration))

            # remove database connection
            # memory cleanup
            del all_data_append
            file_list.clear()
            teradataml.remove_context()
            td_conn.dispose()

            end_time = datetime.datetime.now()
            print('End Time: ' + str(end_time))

            duration = end_time - start_time
            print('Total duration HH:MM:SS.MS: ' + str(duration))
