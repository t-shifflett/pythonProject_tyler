# title: IEX_ETL_SERFF_DATA_BENEFITS_COST_SHARE_VARIANCES_PLAN_VARIANT_BENEFITS.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date: 7/14/2021
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
file_name = r'DATA_BENEFITS'
excel_sheet_names = ['Cost Share Variances',  'Cost',  'Share',  'Variances']
excel_sheet_names_2 = ['Benefits Package',  'Benefits',  'Package']
sheet_name = 'COST_SHARE_VARIANCES'
sheet_table_name = 'PLAN_VARIANT_BENEFITS'
table_name = f'{source}_{file_name}_{sheet_name}_{sheet_table_name}'
td_table_name_stg = f'{table_name}_STG'
td_table_name_t = f'{table_name}_T'
file_list = []  # list of files
file_list_cnt = []  # list of files

# Landing database
if_exists = 'append'

path = variables.file_path

file_processing_start_time = datetime.datetime.now()
print(f'File processing start Time: {file_processing_start_time}')

for root, directories, files in os.walk(path, topdown=False):
    for name in files:
        if name.endswith(variables.file_extensions):
            if name.__contains__(file_name):
                root_name = (os.path.join(root, name))
                file_list_cnt.append(root_name)

print(str(len(file_list_cnt)) + ' files found')

all_data_append = pd.DataFrame()

for root, directories, files in os.walk(path, topdown=False):
    for name in files:
        if name.endswith(variables.file_extensions):
            if name.__contains__(file_name):
                root_name = (os.path.join(root, name))

                print(f'processing: {root_name}')

                excelfile_df = pd.ExcelFile(root_name)
                sheet_names = excelfile_df.sheet_names

                filtered_sheet_list = [x for x in sheet_names if all(y in x for y in excel_sheet_names)]

                print(filtered_sheet_list)

                filtered_sheet_list_2 = [x for x in sheet_names if all(y in x for y in excel_sheet_names_2)]

                excel_pivot_df = pd.read_excel(root_name,  sheet_name=filtered_sheet_list_2[0],  header=None)

                col0_header = 'network_template_version'
                col0_value = excel_pivot_df.iloc[0, 0]

                if col0_value.find('.') >= 0:
                    col0_value = col0_value[:(col0_value.find('.')) + 2]
                else:
                    col0_value = col0_value

                col2_header = 'hios_issuer_id'
                col2_value = excel_pivot_df.iloc[1,  1]

                col1_header = 'issuer_state'
                col1_value = excel_pivot_df.iloc[2,  1]

                col3_header = 'market_coverage'
                col3_value = excel_pivot_df.iloc[3,  1]

                col4_header = 'dental_only_plan'
                col4_value = excel_pivot_df.iloc[4,  1]

                col5_header = 'tin'
                col5_value = excel_pivot_df.iloc[5,  1]

                excel_read_df = pd.concat(pd.read_excel(root_name, sheet_name=filtered_sheet_list, header=None))

                header_loc = pd.DataFrame(
                    [excel_read_df[excel_read_df[excel_read_df.columns[0]] == 'Plan Cost Sharing Attributes'].index[
                         0]])

                header_row_num = header_loc.iloc[0, 1] + 1

                excel_read_df = pd.concat(
                    pd.read_excel(root_name, sheet_name=filtered_sheet_list, header=header_row_num))

                for sheet_name in filtered_sheet_list:

                    print(root_name)
                    print(sheet_name)

                    col_data = pd.read_excel(root_name,
                                                       sheet_name=sheet_name,
                                                       header=None,
                                                       nrows=(header_loc.iloc[0, 1] + 1) + 1)

                    col_data = col_data.fillna(method='pad', axis=1)

                    col_data = col_data.replace('All fields with an asterisk (*) are required', '')
                    #col_data = col_data.replace(['\*'], '', regex=True)
                    #col_data = col_data.replace(['\(', '/','\-'], ' ', regex=True)
                    #col_data = col_data.replace(['\*', r'\n', '\)', '\+', '\?', ','], '', regex=True)
                    #col_data = col_data.replace('&', 'and', regex=True)
                    #col_data = col_data.replace(' ', '_', regex=True)
                    #col_data = col_data.replace('__', '_', regex=True)

                    col_data = col_data.fillna('', axis=1)

                    cols = col_data.iloc[0] + '' + col_data.iloc[1] + '' + col_data.iloc[2]

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

                    len_col_range_list = len(col_range_list)

                    all_data = pd.DataFrame()

                    sheet_name_col = excel_read_df.index.get_level_values(0)

                    for i in range(0, len_col_range_list):
                        x = i
                        y = i + 1
                        num_range = range(col_range_list[x], col_range_list[y])

                        num_list = list(num_range)

                        num_list.insert(0, 0)

                        excel_read_df = pd.read_excel(root_name, sheet_name=sheet_name,
                                                                header=header_row_num,
                                                                usecols=num_list)

                        cols = col_data.iloc[:, num_list]

                        excel_read_df.columns = cols.columns

                        col_cnt = len(excel_read_df.columns)
                        sheet_name_col = sheet_name
                        excel_read_df.insert(col_cnt, 'sheetname', sheet_name_col)

                        benefit_name = cols.iloc[0, 1]

                        print(f'processing: {benefit_name}')

                        id_vars = cols.columns[0]

                        value_vars = cols.columns[1]

                        excel_read_df_melt_1 = pd.melt(excel_read_df,
                                                     id_vars=id_vars,
                                                     var_name='benefit',
                                                     value_vars=cols.columns[1],
                                                     value_name='copay_in_network_tier_1'
                                                     )

                        excel_read_df_melt_1.insert(2, 'benefit_name', benefit_name)
                        excel_read_df_melt_1 = excel_read_df_melt_1 .drop(columns='benefit', axis=1)

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

                        excel_read_df_melt_7 = pd.melt(excel_read_df,
                                                       id_vars=id_vars,
                                                       var_name='benefit',
                                                       value_vars=excel_read_df.columns[7],
                                                       value_name='sheet_name')

                        excel_read_df_melt_7.insert(2, 'benefit_name', benefit_name)
                        excel_read_df_melt_7 = excel_read_df_melt_7.drop(columns='benefit', axis=1)

                        excel_read_df_melt_merge = pd.merge(excel_read_df_melt_1, excel_read_df_melt_2,
                                                            on=[id_vars, 'benefit_name'])
                        excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_3,
                                                            on=[id_vars, 'benefit_name'])
                        excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_4,
                                                            on=[id_vars, 'benefit_name'])
                        excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_5,
                                                            on=[id_vars, 'benefit_name'])
                        excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_6,
                                                            on=[id_vars, 'benefit_name'])
                        excel_read_df_melt_merge = pd.merge(excel_read_df_melt_merge, excel_read_df_melt_7,
                                                            on=[id_vars, 'benefit_name'])

                        all_data = all_data.append(excel_read_df_melt_merge, ignore_index=True)

                        for x in range(1, 8):
                            excel_read_df_melt = f'excel_read_df_melt_{x}'
                            del excel_read_df_melt

                        del excel_read_df_melt_merge
                        del benefit_name

                        if y == (len_col_range_list - 1):
                            break

                    # Data manipulation:
                    all_data.insert(0, col5_header, col5_value)
                    all_data.insert(0, col4_header, col4_value)
                    all_data.insert(0, col3_header, col3_value)
                    all_data.insert(0, col2_header, col2_value)
                    all_data.insert(0, col1_header, col1_value)
                    all_data.insert(0, col0_header, col0_value)

                    all_data.insert(0, 'serff_tracking_number', root_name)  # insert column at the end
                    all_data['serff_tracking_number'] = all_data['serff_tracking_number']. \
                        str.slice(variables.serff_tracking_number_slice_start,
                                  variables.serff_tracking_number_slice_stop)

                    col_cnt = len(all_data.columns)  # column count starting at 0
                    all_data.insert(col_cnt, 'src_file_desc', root_name)  # insert column at the end
                    all_data['src_file_desc'] = all_data['src_file_desc']. \
                        str.slice(start=variables.src_file_desc_slice_start)  # start reading string

                    all_data.insert(0, 'plan_yr', col0_value)
                    all_data['plan_yr'] = all_data['plan_yr'].str.slice(stop=4)  # stop reading string after
                    # 27 characters

                    col_cnt = len(all_data.columns)  # column count starting at 0
                    all_data.insert(col_cnt, 'src_file_provided_source_date', start_time)
                    # insert column at the end

                    col_cnt = len(all_data.columns)  # column count starting at 0
                    all_data.insert(col_cnt, 'row_status', 1)  # insert column at the end

                    col_cnt = len(all_data.columns)  # column count starting at 0
                    all_data.insert(col_cnt, 'change_date', start_time)  # insert column at the end

                    col_cnt = len(all_data.columns)  # column count starting at 0
                    all_data.insert(col_cnt, 'change_by', 'source')  # insert column at the end

                    col_cnt = len(all_data.columns)  # column count starting at 0
                    all_data.insert(col_cnt, 'load_dt', start_time)  # insert column at the end

                    all_data = all_data.rename(columns={all_data.columns[8]:
                                               'plan_cost_sharing_attributes_hios_plan_id_standard_component_variant'})

                    all_data_append = all_data_append.append(all_data, ignore_index=True)
                    file_list.append(root_name)

file_name_df = pd.DataFrame(file_list, columns=['Root_File_Name'])

# data to csv
all_data_append.to_csv(f'csv/{table_name}.csv')
file_name_df.to_csv(f'csv/{table_name}_file_list.csv')

file_processing_end_time = datetime.datetime.now()

file_processing_duration = file_processing_end_time - file_processing_start_time
print('File processing duration HH:MM:SS.MS: ' + str(file_processing_duration))

df_file_list_cnt = len(file_name_df.index)
print(f'{df_file_list_cnt} files processed')

all_data_append = all_data_append.astype(str)  # change all data to str to load into teradata to prevent load errors with different

all_data_append = all_data_append.replace(['nan', 'NaN'], [None, None])

datetime64_cols = ['src_file_provided_source_date',  'change_date',  'load_dt']
date_col = ['plan_effective_date',  'plan_expiration_date']
int_cols = ['plan_yr',  'row_status']

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
    # use copy_to_sql if rowcount is over 100k https://docs.teradata.com/r/GsM0pYRZl5Plqjdf9ixmdA/pvQq0OGIULWr5dtBfDreUg

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
