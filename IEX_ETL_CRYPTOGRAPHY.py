# title: IEX_ETL_CRYPTOGRAPHY.py
# this file location: https://github.optum.com/IEX/IEX_PY/tree/master/PROD
# author: tyler shifflett
# initial creation date:5/3/2021
# last updated date: github will have latest date

# desc: cryptography tool to store encrypted pws
# documentation https://github.com/pyca/cryptography

from cryptography_key import td_key
from cryptography_token import td_token
from cryptography.fernet import Fernet

# Teradata
f = Fernet(td_key)
token = td_token
decrypted_token = f.decrypt(token)

td_pwd = bytes(decrypted_token).decode("utf-8")
