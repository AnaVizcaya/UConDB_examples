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

#To-Do
'''
If there is a problem with the run, the script will end after cheking metadata. Insert error in DB?
Check what's the problem with 12003,4,
'''

def run(args):
    '''
    Main run method
    '''
    rcd = runConditionsData(args.run)
    
    folder = connect_to_con()
    #Add data
    chunk = []
    channel = rcd.run_number
    tv = 0
    chunk.append((channel, tv, rcd.start_time, rcd.end_time, rcd.daqmeta_dict['DETECTOR_ID'], rcd.daqmeta_dict['RUN_TYPE'], rcd.daqmeta_dict['SOFTWARE_VERSION'], rcd.daqconfig_dict['buffer'], rcd.daqconfig_dict['ac_couple'], rcd.daqconfig_dict['pulse_mode'], rcd.daqconfig_dict['pulse_dac'], rcd.daqconfig_dict['pulser'], rcd.daqconfig_dict['baseline_high'], rcd.daqconfig_dict['baseline'], rcd.daqconfig_dict['gain'], rcd.daqconfig_dict['leak'], rcd.daqconfig_dict['leak_high'], rcd.daqconfig_dict['leak_10x'], rcd.daqconfig_dict['shape'], rcd.daqconfig_dict['peak_time'], rcd.daqconfig_dict['enable_femb_fake_data'], rcd.daqconfig_dict['enabled'], rcd.daqconfig_dict['test_cap'], rcd.daqconfig_dict['daq_config_name']))
    #column = ['start_time', 'stop_time', 'detector_id', 'run_type']
    column = ['start_time', 'stop_time', 'detector_id', 'run_type', 'software_version', 'buffer', 'ac_couple', 'pulse_mode', 'pulse_daq', 'pulser', 'baseline_high', 'baseline', 'gain', 'leak', 'leak_high', 'leak_10x', 'shape', 'peak_time', 'enable_femb_fake_data', 'enabled', 'test_cap', 'daq_config_name']
    #folder.addData(chunk, columns = column)   
    '''
    print(folder.data_column_types())
    data = folder.getData(0) #, channel_range=(12019, 12020))
    for row in data:
        print(f'{row}')
    '''
    return

class runConditionsData():
    '''
    This class will get the configuration blob from the UconDB, 
    and extract a subset of data that can be used for offline analysis.
    Finally it will send the subset of data into a new run history DB 
    '''

    def __init__(self, run_number, verbose = 2, ucon_folder = 'protodune_conditions',
                  ucon_object = 'configuration_all'):
        '''
        :param run_number: int
             number of run which we want to obtain configuration info
        :param verbose: int (1,2,3), optional
            how much info do you want printed. 1 for less, 3 for more
        :param ucon_object: str, optional
            object in the UconDB where the blob will be send, some options (test, protodune_conditions)
        :param ucon_folder: str, optional
            folder in the UconDB where the blob will be send, some options (testt, configuration_all)
        '''
        self.run_number = run_number
        self.verbose = verbose
        self.ucon_folder = ucon_folder
        self.ucon_object = ucon_object
        self.blob_str = 'blob_' + str(self.run_number) + '.txt'
        self.ucondb_url = 'https://dbdata0vm.fnal.gov:9443/protodune_ucon_prod/app/data/' + self.ucon_folder + '/' + self.ucon_object + '/key=' + str(self.run_number)
        #Get blob
        self.daq_data_blob = self.get_daqblob()


        #Get specific daq metadata        
        self.daqmeta_obj = daqMetadata(self.run_number, self.daq_data_blob)
        self.daqmeta_dict = self.daqmeta_obj.return_dict()
        self.start_time = self.daqmeta_dict['START_TIME'] 
        self.end_time   = self.daqmeta_dict['STOP_TIME']

        #Get specific daq config data 
        self.daqconfig_obj = daqConfigData(self.run_number, self.daq_data_blob)
        self.daqconfig_dict = self.daqconfig_obj.return_dict()
        '''
        self.daq_config
        self.slowC_data
        self.ifbeam_data
        '''
        return

    def get_daqblob(self):
        '''
        Get the blob from the UconDB. Using their API which has to be installed. But it can also be retrieved with curl 
        '''
        # IF the file is already in the folder
        for files in os.listdir('.'):
            if self.blob_str in files:
                if self.verbose >= 1:
                    print(f'Using the file {self.blob_str} found in this folder.')
                with open(self.blob_str) as blob_f:
                    blob = blob_f.readlines()
                return blob
        # Using curl
        com = 'curl ' + self.ucondb_url + ' > ' + self.blob_str
        ret_code = subprocess.run(com, shell=True )
        with open(self.blob_str) as blob_f:
            blob = blob_f.readlines()
        return blob
        # Using ucondb client tool 
        '''
        client = UConDBClient("https://dbdata0vm.fnal.gov:9443/protodune_ucon_prod/app")
        try:
            data_dict = client.get( self.ucon_folder, self.ucon_object, key=str(self.run_number), meta_only=False)
        except:
            print(f'The blob of run {self.run_number} could not be retrieved from UconDB')
            return 0
        
        if self.verbose >= 1:
            print(f'The blob of daq data of run {self.run_number} was retrieved successfully')
        if self.verbose >= 3: 
            print(f"The metadata of {self.ucon_object} and run {self.run_number} is: {data_dict['data']}")
        return data_dict['data']
        '''

class daqMetadata():
    '''
    This class will extract all the daq metadata info of the run. 
    And get the data in the correct data type
    '''
     
    def __init__(self, run_number, daq_data_blob, verbose = 3):
        '''
        :param daq_data_blob: str
            blob with all the daq data
        :param verbose: int
            how much info do you want printed. 1 for less, 3 for more
        '''
        self.run_number = run_number
        self.blob = daq_data_blob
        self.verbose = verbose
        self.meta_dict = {}
        #self.init_time

        #Get dict of daq metadatan and put them in the correct data type 
        self.unpack_meta()
        self.correct_type()
        return 

    def unpack_meta(self):
        '''
        Unpacking the metadata from the blob and creating a dictionary with the info
        '''
        if self.verbose >= 2:
            print(f'Unpacking the metadata of run {self.run_number}.')
        line_number = 0
        for line in self.blob:
            #print(line)
            line_number += 1
            if 'runMeta'in line:
                meta_line = self.blob[line_number+1]
        meta_brack = re.findall( r"\[.*?\]", meta_line)
        meta_key = re.findall(r'"(.*?)"', meta_brack[0])
        if len(meta_key) <= 1:
            print(f'Problem with run {self.run_number}')
            exit(0)
        meta_value = re.findall(r'"(.*?)"', meta_brack[1])
        meta_all_values = re.sub(r'([a-km-z]),', r'\1', meta_brack[1])
        meta_all_values = re.sub('"', '', meta_all_values)
        meta_all_values = re.sub(']', '', meta_all_values)
        meta_all_values = re.sub('\[\[', '', meta_all_values)
        meta_all_values = meta_all_values.split(",")
        if self.verbose >= 2:
            print(f'The meta keys are: {meta_key} and the values: {meta_all_values}.')
        if self.verbose >= 1:
            print(f'Filling the dictionary with the run metadatada.')
        for i in range(0, len(meta_key)):
            self.meta_dict[meta_key[i]] = meta_all_values[i]
        if self.verbose >= 3:
            print(f'The dictionary with metadatad is: {self.meta_dict}')
        return 

    def correct_type(self):
        '''
        Get the data in the metadata daq dictionary to the correct data type
        '''
        for key in self.meta_dict:
            if key == 'RUN_NUMBER':
                self.meta_dict[key] = int(self.meta_dict[key])
            #if 'TIME' in key:
            if self.meta_dict[key] == 'null':
                self.meta_dict[key] = None
             #       return
             #   self.meta_dict[key] = datetime.strptime(self.meta_dict[key], '%a %d %b %Y %H:%M:%S %Z')
        if self.verbose >= 2:
            print(f'The updated daq metadata key is: {self.meta_dict}')
        return

    def return_dict(self):
        '''
        Gives the meta dict
        '''
        return self.meta_dict

class daqConfigData():
    '''
    Get the config data of just some parameters
    '''
   
    def __init__ (self, run_number, daq_data_blob, verbose = 2):
        '''
        :param daq_data_blob: str
            blob with all the daq data
        :param verbose: int
            how much info do you want printed. 1 for less, 3 for more
        '''
        self.run_number = run_number
        self.blob = daq_data_blob
        self.verbose = verbose
        self.config_dict = {}
        self.config_name_line = ''
        self.conf_dict = self.unpack_config()
        self.list_param = ['buffer', 'ac_couple', 'pulse_mode', 'pulse_dac', 'pulser', 'baseline_high', 'baseline', 'gain', 'leak', 'leak_high', 'leak_10x', 'shape', 'peak_time', 'enable_femb_fake_data', 'enabled', 'test_cap']
        for daq_conf in self.list_param:
            self.extract_config_conditions(daq_conf)
        '''
        self.extract_config_conditions('buffer')
        self.extract_config_conditions('ac_couple')
        self.extract_config_conditions('pulse_mode')
        self.extract_config_conditions('pulse_dac')
        self.extract_config_conditions('pulser')
        self.extract_config_conditions('baseline_high')
        self.extract_config_conditions('baseline')
        self.extract_config_conditions('gain')
        self.extract_config_conditions('leak')
        self.extract_config_conditions('leak_high')
        self.extract_config_conditions('leak_10x')
        self.extract_config_conditions('shape')
        self.extract_config_conditions('peak_time')
        self.extract_config_conditions('enable_femb_fake_data')
        self.extract_config_conditions('enabled')
        self.extract_config_conditions('test_cap')
        '''
        self.name_of_file()
        if self.verbose >= 2:
            print(f'The config dict is: {self.config_dict}')
        return

    def unpack_config(self):
        '''
        Unpacking the config data from the blob and create a dict with the info
        '''
        if self.verbose >= 2:
            print(f'Creating the python dictionary with the DAQ coldbox config settings info.')
        line_number = 0
        line_number1 = 0
        for line in self.blob:
            line_number += 1
            if line.startswith(str(self.run_number)):
                if re.search('wib\d\d1_conf', line) or re.search('_\d\d1_conf', line):
                    self.config_name_line = line
                    if self.verbose >= 2:
                        print(f'Extracting data from the file: {line}')
                    start_blob = line_number + 2
                    line_number1 = line_number
                    for line1 in self.blob[line_number+1:]:
                        #print(line1)
                        if '#####' in line1:
                            line_number1 += 1
                            end_blob = line_number1
                            break
                        else:
                            line_number1 += 1
        wib_conf = self.blob[start_blob-1:end_blob]
        if self.verbose >= 2:
            print(f'The start of the blob is: {start_blob} and the end of the blob line is: {end_blob}')
        #wib_conf = self.blob[1:2]
        wib_str = ''
        for x in wib_conf:
            if self.verbose >=3:
                print(x)
            wib_str += x
        wib_str = wib_str.replace('"modules": [', '"modules": ')
        wib_str = wib_str.replace(']\n', '')
        wib_str = wib_str.replace(']}', '}')
        wib_conf_dict = json.loads(wib_str)
        if self.verbose >= 3:
            print(f'The whole DAQ config dictionary is: {wib_conf_dict}.')
        return wib_conf_dict

    def extract_config_conditions(self, param):
        '''
        Extract the parameter that will go to the conditions DB from the DAQ coldbox config dictionary
        '''
        if self.verbose >= 2:
            print(f'Extracting the value of parameter {param}')
        value = 'null'
 
        for k, v in recursive_items(self.conf_dict):
            if re.fullmatch(param,k):
                value = v
                break
            if param == 'buffer':
                if re.fullmatch('buffering',k):
                    value = v
                    break
        if self.verbose >= 3:
            print(f'The param {k} and the value {v}')        
        if value == 'null':
            value = None
        self.config_dict[param] = value  
        return

    def name_of_file(self):
        '''
        Find out the name of the config file
        '''
        fn = self.config_name_line
        names = re.split(r'/', fn)
        name = names[-1]
        name = name.replace('\n', '')
        if self.verbose >= 2:
            print(f'The config file name is: {name}')
        self.config_dict['daq_config_name'] = name
        return

    def return_dict(self):
        '''
        Gives the meta dict
        '''
        return self.config_dict

#######        
def recursive_items(dictionary):
    '''
    Function to access all the key:value pairs in the dict
    '''
    for key, value in dictionary.items():
        if type(value) is dict:
            yield (key, value)
            yield from recursive_items(value)
        else:
            yield (key, value)

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

