# title: variables.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date: 05/07/2021
# last updated date: github will have latest date

# desc: contains all reusable variables for project.

import pytz
import platform

current_os = platform.system()

windows_file_path = r'C:\Users\tshiffl1\OneDrive - UHG\IEX_DB\SERFF'
windows_file_path_cms = r'C:\Users\tshiffl1\OneDrive - UHG\IEX_DB\CMS'
windows_test_file_path = r'C:\Users\tshiffl1\OneDrive - UHG\IEX_DB\Test\2022'
windows_test_file_path_2 = r'C:\Users\tshiffl1\OneDrive - UHG\IEX_DB\Test\Test'
linux_file_path = r'/app1/serff'
linux_file_path_cms = r'/app1/cms'
linux_py_scripts_path = r'/app1/PROD-env/IEX_PY/PROD/'

if current_os == 'Windows':
    file_path = windows_file_path
    file_path_cms = windows_file_path_cms
    serff_tracking_number_slice_start = 54
    serff_tracking_number_slice_stop = 73
    src_file_desc_slice_start = 54
else:
    file_path = linux_file_path
    file_path_cms = linux_file_path_cms
    serff_tracking_number_slice_start = 20
    serff_tracking_number_slice_stop = 39
    src_file_desc_slice_start = 20

file_extensions = ('.xls', '.xlsm', '.xlsb', '.xlsx','.csv')  # tuple list of file extensions
cst = pytz.timezone('US/Central')


def standard_cols_end(x):
    print("standard_cols_end" + str(x))
