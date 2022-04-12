# title: Teradata_Connect.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date: 05/07/2021
# last updated date: github will have latest date

# desc: contains all teradata connection settings for project.

import teradataml
import IEX_ETL_CRYPTOGRAPHY as IEX_ETL_CRYPTOGRAPHY
import getpass
import sqlalchemy

# Landing Database Parameters
database = "UDWDATALAB_IEX"
# user = getpass.getuser()
user = 'tshiffl1'
password = IEX_ETL_CRYPTOGRAPHY.td_pwd
host = 'UDWPROD.UHC.COM'

db_success = f'Database connection successful for {user}!'

td_conn = teradataml.create_context(host=host, username=user, password=password, logmech='LDAP', database=database)
print(f'{db_success} on DB teradataml string')

# sqlalchemy_engine = \
#         sqlalchemy.create_engine(f'teradatasql://{host}/?user={user}&password={password}&logmech=LDAP&database={database}')
# td_conn = teradataml.create_context(tdsqlengine=sqlalchemy_engine)
# print(f'{db_success} on DB sqlalchemy string')
