# title: IEX_ETL_SERFF_DATA_ECP_NETWORK_ADEQUACY_INDIVIDUAL_PROVIDERS.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date:9/01/2021
# last updated date: github will have latest date

# desc: ETL Tool to extract data from excel files and insert data into Teradata

from Teradata_Connect import td_conn
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
file_name = r'DATA_ECP_NETWORK_ADEQUACY'
excel_sheet_names = ['IndividualProviders']
excel_sheet_names_2 = ['User Control']
sheet_name = 'INDIVIDUAL_PROVIDERS'
table_name = f'{source}_{file_name}_{sheet_name}'
td_table_name_stg = f'{table_name}_STG'
td_table_name_t = f'{table_name}_T'
file_list = []  # list of files
file_list_cnt = []  # list of files

# Landing database
if_exists = 'replace'

path = variables.file_path

all_data = pd.DataFrame()

file_processing_start_time = datetime.datetime.now()
print(f'File processing start Time: {file_processing_start_time}')

for root, directories, files in os.walk(path, topdown=False):
    for name in files:
        if name.endswith(variables.file_extensions):
            if name.__contains__(file_name):
                root_name = (os.path.join(root, name))
                file_list_cnt.append(root_name)

print(str(len(file_list_cnt)) + ' files found')

for root, directories, files in os.walk(path, topdown=False):
    for name in files:
        if name.endswith(variables.file_extensions):
            if name.__contains__(file_name):
                root_name = (os.path.join(root, name))

                print(f'processing: {root_name}')

                excelfile_df = pd.ExcelFile(root_name)
                sheet_names = excelfile_df.sheet_names

                filtered_sheet_list = [x for x in sheet_names if all(y in x for y in excel_sheet_names)]  # get
                # excel sheet names and store in list

                filtered_sheet_list_2 = [x for x in sheet_names if all(y in x for y in excel_sheet_names_2)]  # get
                # excel sheet names 2 and store in list

                excel_pivot_df = pd.read_excel(root_name, sheet_name=filtered_sheet_list_2[0], header=None)

                col0_header = 'template_version'
                col0_value = excel_pivot_df.iloc[1, 2]

                if col0_value.find('.') >= 0:
                    col0_value = col0_value[:(col0_value.find('.')) + 2]
                else:
                    col0_value = col0_value

                col2_header = 'issuer_id'
                col2_value = excel_pivot_df.iloc[5, 3]

                col1_header = 'issuer_state'
                col1_value = 'ADD LATER'

                filtered_sheet_list_length = len(filtered_sheet_list)

                print(f'elements in list: {filtered_sheet_list_length}')

                print(filtered_sheet_list)

                if filtered_sheet_list_length == 0:
                    print('No sheet exists')
                else:
                    excel_read_df = pd.concat(pd.read_excel(root_name, sheet_name=filtered_sheet_list, header=None))

                    # header_loc = pd.DataFrame(
                    #     [excel_read_df[
                    #          excel_read_df[excel_read_df.columns[0]] == 'National Provider Identifier (NPI)*'].index[0]])

                    header_loc = 1

                    excel_read_df = pd.concat(
                        pd.read_excel(root_name, sheet_name=filtered_sheet_list, header=header_loc))

                    col_data = pd.concat(pd.read_excel(root_name,
                                                       sheet_name=filtered_sheet_list,
                                                       header=None,
                                                       skiprows=1,
                                                       nrows=1))

                    # col_data = col_data.fillna(method='pad', axis=1)

                    col_data = col_data.replace('All fields with an asterisk (*) are required', '_')
                    col_data = col_data.replace(['\(', '/', '-'], ' ', regex=True)
                    col_data = col_data.replace(['\*', r'\n', '\)', '\+', '\?'], '', regex=True)
                    col_data = col_data.replace(' ', '_', regex=True)
                    col_data = col_data.replace('__', '_', regex=True)
                    col_data = col_data.replace('e.g.,', 'eg', regex=True)

                    cols = col_data.iloc[0]

                    cols = cols.replace('__', '', regex=True)

                    cols = cols.str.lower()

                    cols = cols.str.slice(stop=128)  # Teradata column width max limit is 128

                    excel_read_df.columns = cols

                    excel_read_df = excel_read_df.dropna(how='all')

                    # Data manipulation:
                    excel_read_df.insert(0, col2_header, col2_value)
                    excel_read_df.insert(0, col1_header, col1_value)
                    excel_read_df.insert(0, col0_header, col0_value)

                    excel_read_df.insert(0, 'serff_tracking_number', root_name)  # insert column at the end
                    excel_read_df['serff_tracking_number'] = excel_read_df['serff_tracking_number']. \
                        str.slice(variables.serff_tracking_number_slice_start,
                                  variables.serff_tracking_number_slice_stop)

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    sheet_name_col = excel_read_df.index.get_level_values(0)
                    excel_read_df.insert(col_cnt, 'sheet_name', sheet_name_col)

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    excel_read_df.insert(col_cnt, 'src_file_desc', root_name)  # insert column at the end
                    excel_read_df['src_file_desc'] = excel_read_df['src_file_desc']. \
                        str.slice(start=variables.src_file_desc_slice_start)  # start reading string

                    excel_read_df.insert(0, 'plan_yr', col0_value)
                    excel_read_df['plan_yr'] = excel_read_df['plan_yr'].str.slice(stop=4)  # stop reading string after
                    # 27 characters

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    excel_read_df.insert(col_cnt, 'src_file_provided_source_date', start_time)
                    # insert column at the end

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    excel_read_df.insert(col_cnt, 'row_status', 1)  # insert column at the end

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    excel_read_df.insert(col_cnt, 'change_date', start_time)  # insert column at the end

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    excel_read_df.insert(col_cnt, 'change_by', 'source')  # insert column at the end

                    col_cnt = len(excel_read_df.columns)  # column count starting at 0
                    excel_read_df.insert(col_cnt, 'load_dt', start_time)  # insert column at the end

                    try:
                        excel_read_df = excel_read_df.rename(
                            columns={'national_provider_number_npi': 'national_provider_identifier_npi',
                                     'national_provider_identifier_npi_': 'national_provider_identifier_npi',
                                     'national_provider_number_npi_': 'national_provider_identifier_npi'})
                    except:
                        print('no columns to be renamed')

                    print(excel_read_df)

                    # combine all datasets into one
                    all_data = all_data.append(excel_read_df, ignore_index=True)
                    file_list.append(root_name)

# removes unamed columns
all_data.drop(all_data.columns[all_data.columns.str.contains('unnamed', case=False, na=False)], axis=1, inplace=True)
try:
    all_data.drop(columns=['Blank'], inplace=True)
except:
    print('no blank columns')

file_name_df = pd.DataFrame(file_list, columns=['Root_File_Name'])

# data to csv
all_data.to_csv(f'csv/{table_name}.csv')
file_name_df.to_csv(f'csv/{table_name}_file_list.csv')

file_processing_end_time = datetime.datetime.now()

file_processing_duration = file_processing_end_time - file_processing_start_time
print('File processing duration HH:MM:SS.MS: ' + str(file_processing_duration))

df_file_list_cnt = len(file_name_df.index)
print(f'{df_file_list_cnt} files processed')

all_data = all_data.astype(str)  # change all data to str to load into teradata to prevent load errors with different
# data types

all_data = all_data.replace(['nan', 'NaN'], [None, None])

datetime64_cols = ['src_file_provided_source_date', 'change_date', 'load_dt']
date_col = ['plan_effective_date', 'plan_expiration_date']
int_cols = ['plan_yr', 'row_status']

print(all_data.info())
print(all_data)

df_cnt = len(all_data.index)
print(f'DataFrame count: {df_cnt}')

database_start_time = datetime.datetime.now()
print(f'Database start Time: {database_start_time}')

truncate_stg = f'DELETE FROM {td_table_name_stg};'
td_conn.execute(truncate_stg)

# if df_cnt < 100000:
teradataml.copy_to_sql(df=all_data, table_name=td_table_name_stg, if_exists=if_exists, index=True,
                           index_label='row_id')
    # use copy_to_sql if rowcount is less than 100k
# else:
#     teradataml.fastload(df=all_data, table_name=td_table_name_stg, if_exists=if_exists, index=True,
#                         index_label='row_id', batch_size=200000, primary_index={})
#     # use copy_to_sql if rowcount is over 100k https://docs.teradata.com/r/GsM0pYRZl5Plqjdf9ixmdA/pvQq0OGIULWr5dtBfDreUg

count_stg = f'select count(*) from {td_table_name_stg}'
result_stg = td_conn.execute(count_stg)

load_msg = f'{td_table_name_stg} has been loaded with {count_stg} records'
print(load_msg)

truncate_t = f'DELETE FROM {td_table_name_t};'
td_conn.execute(truncate_t)

insert_into_t = f'INSERT INTO {td_table_name_t} SELECT * FROM {td_table_name_stg};'
td_conn.execute(insert_into_t)

#td_conn.execute(truncate_stg)

count_t = f'select count(*) from {td_table_name_t}'
result_t = td_conn.execute(count_t)

for row in result_t:
    print(f'{td_table_name_t} has been loaded with {row} records')

database_end_time = datetime.datetime.now()

database_duration = database_end_time - database_start_time
print('Database duration HH:MM:SS.MS: ' + str(database_duration))

# remove database connection
# memory cleanup
del all_data
file_list.clear()
teradataml.remove_context()
td_conn.dispose()

end_time = datetime.datetime.now()
print('End Time: ' + str(end_time))

duration = end_time - start_time
print('Total duration HH:MM:SS.MS: ' + str(duration))
