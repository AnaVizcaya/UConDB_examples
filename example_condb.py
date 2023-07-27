import os, sys, time, subprocess
import argparse
import re
import json
import pandas as pd
from datetime import datetime
import pprint
from ucondb.webapi import UConDBClient
from condb import ConDB
from condb import CDFolder

def run(args):
    '''
    Main run method
    '''
    rcd = runConditionsData(args.run) #This class not included in the script, but is just to get the data
    
    folder = connect_to_con()
    #Add data
    chunk = []
    channel = rcd.run_number
    tv = 0
    chunk.append((channel, tv, rcd.start_time, rcd.end_time, rcd.daqmeta_dict['DETECTOR_ID'], rcd.daqmeta_dict['RUN_TYPE'], rcd.daqmeta_dict['SOFTWARE_VERSION'], rcd.daqconfig_dict['buffer'], rcd.daqconfig_dict['ac_couple'], rcd.daqconfig_dict['pulse_mode'], rcd.daqconfig_dict['pulse_dac'], rcd.daqconfig_dict['pulser'], rcd.daqconfig_dict['baseline_high'], rcd.daqconfig_dict['baseline'], rcd.daqconfig_dict['gain'], rcd.daqconfig_dict['leak'], rcd.daqconfig_dict['leak_high'], rcd.daqconfig_dict['leak_10x'], rcd.daqconfig_dict['shape'], rcd.daqconfig_dict['peak_time'], rcd.daqconfig_dict['enable_femb_fake_data'], rcd.daqconfig_dict['enabled'], rcd.daqconfig_dict['test_cap'], rcd.daqconfig_dict['daq_config_name']))
    column = ['start_time', 'stop_time', 'detector_id', 'run_type', 'software_version', 'buffer', 'ac_couple', 'pulse_mode', 'pulse_daq', 'pulser', 'baseline_high', 'baseline', 'gain', 'leak', 'leak_high', 'leak_10x', 'shape', 'peak_time', 'enable_femb_fake_data', 'enabled', 'test_cap', 'daq_config_name']
    folder.addData(chunk, columns = column)   
    # Inspect the data
    print(folder.data_column_types())
    data = folder.getData(0) #, channel_range=(12019, 12020))
    for row in data:
        print(f'{row}')
    return

######## conditons db functions 
def connect_to_con():
    '''
    Connect to the condb and add to the run history table
    '''
    host    = 'ifdbprod2.fnal.gov'
    port    = '5451'
    db_name = 'pdunesp_prod'
    user    = 'avizcaya'
    connstr = f'host={host} port={port} dbname={db_name} user={user}'
    db = ConDB(connstr = connstr)
    return db.openFolder('pdunesp.test_prueba')



if __name__ == '__main__': 
    parser = argparse.ArgumentParser(description='Access conditions data from the UconDB and send to run config DB')
    parser.add_argument("--run", type=int, default=12020, help="Run number to extract info")
    args = parser.parse_args()
    run(args)

