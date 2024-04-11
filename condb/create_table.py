import os
import subprocess
import re

# Table information
table_name = 'pdunesp.test_prueba' #schema.table_name
host = 'ifdbprod2.fnal.gov'
port = '5451'
user = ####
passw = ####
r_permission = 'pdunesp_web' # DB users to grant read permissions to
w_permission = 'pdunesp_web' #DB users to grant write permissions to
database = 'pdunesp_prod'
#Payloads format 'column_name_1:data_type column_name_2:data_type' add all the column names that should be included in the table
payloads = 'upload_time:float start_time:float stop_time:float run_type:text detector_id:text software_version:text extra_condition:hstore'

com = f'condb create -h {host} -p {port} -U {user} -w {passw} -s -R {r_permission} -W {w_permission} {database} {table_name} {payloads}'
# !!!! add -c before -s to force create and drop existing table. but be carefull, because it drops existing table
print(com)
comm = re.split(' ', com)

try:
    subprocess.run(comm)
except:
    print(f'something didnt work with the creation of the table')
print(comm)
