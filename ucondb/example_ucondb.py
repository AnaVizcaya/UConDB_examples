import os, sys, time, subprocess
import argparse
#from ucondb.webapi import UConDBClient
import shutil, re
import pandas as pd
import numpy as np

def run(args):
    '''
    Main run method
    If one run is specified, upload one run
    If metadata is specified, look up new runs uloaded to DAQ db from the metadata
    Else, run on a range of runs
    '''
    if args.run:
        print(f'Working with run {args.run}')
        r = runConfigurations(args.run)
    elif args.runmeta:
        print(f'Working with new runs from DAQ db')
        info = configTransferInfo(args.runinfo, args.runmeta, verbose = 1)
        runs_in_ucondb = []
        for run in reversed(info.runs_not_tr):
            print(f'"Working with run {run}')
            r = runConfigurations(run, to_UconDB = True, verbose = 1, ucon_folder = 'protodune_conditions', ucon_object = 'configuration_all')
            if r.run_in_ucon:
                runs_in_ucondb.append(r.run_number)
        #update_upload_runs_ucon(args.runinfo, info, runs_in_ucondb)
        print(f'The following files where uploaded to UconDB {runs_in_ucondb}')
        for run_tr in runs_in_ucondb:
            info.runs_not_tr.remove(int(run_tr))
        print(f'And the runs not transfered {info.runs_not_tr}. ')
        update_upload_runs_ucon(args.runinfo, info, runs_in_ucondb)
        log_file = "transferArchiveConfigsToUConDB.log"
        log_info = 'The following files where uploaded to UconDB' + str(runs_in_ucondb) + '\n And the runs not transfered ' + str(info.runs_not_tr)
        write_in_log(log_file, log_info)
    else:
        print('Working with a range of runs')
        range_of_runs(18040,18090)
    return

def range_of_runs(i_run, f_run):
    '''
    Send the config info of a range of runs to the UconDB
    '''
    runs_in_ucondb, runs_no_ucondb = [], []
    for run in range(i_run, f_run):
        r = runConfigurations(run, ucon_folder = 'protodune_conditions', ucon_object = 'configuration_all')
        if r.run_in_ucon:
            runs_in_ucondb.append(run)
        else:
            runs_no_ucondb.append(run)
    print(f'The following runs were uploaded to the UconDB: {runs_in_ucondb}.')
    print(f'The following runs were not uploaded to the UconDB: {runs_no_ucondb}.')

    return

class runConfigurations():
    '''
    The runConfigurations class is used to download the runconfig files from the DAQ DB
    And upload them to the UConDB
    '''
    def __init__(self, run_number, verbose = 3, ucon_folder = 'test', ucon_object = 'test',
                 to_UconDB = True, status_upload = True):
        '''
        :param run_number: int
            number of run which we want to obtain configurations
        :param verbose: int (1,2,3), optional
            how much info do you want printed. 1 for less, 3 for more
        :param ucon_object: str, optional
            object in the UconDB where the blob will be send, some options (test, protodune_conditions)
        :param ucon_folder: str, optional
            folder in the UconDB where the blob will be send, some options (testt, configuration_all)
        :param to_UconDB: bool, optional
            Upload the run blob, containing the configuraiton files to the UconDB
        :param status_upload: bool, optional
            Check is the upload was succesful
        '''
        self.run_number = str(run_number)
        self.verbose = verbose
        self.ucon_folder = ucon_folder
        self.ucon_object = ucon_object
        self.to_UconDB = to_UconDB 
        self.status_upload = status_upload
        
        self.run_dir = str(run_number)
        self.blob_str = 'blob_' + self.run_number + '.txt'
        self.ucondb_url = 'https://dbdata0vm.fnal.gov:9443/protodune_ucon_prod/app/data/' + self.ucon_folder + '/' + self.ucon_object + '/key=' + self.run_number
        
        #Download config from DAQ db and create blob
        self.make_dir()
        self.get_config_files()
        self.file_names = self.get_meta_info()
        if self.file_names:
            self.write_blob()
            if self.to_UconDB:
                self.blob_to_uconDB()
        if self.status_upload:
            self.run_in_ucon = self.check_transf()

        #self.remove_dir()


    def make_dir(self):
        """
        Make run directory where the tar file from the DAQ db will be saved
        """
        if self.verbose >= 1:
            print(f'Creating the directory for run {self.run_number}')
        if not os.path.exists(self.run_dir):
            os.makedirs(self.run_dir)
        else:
            print(f'The directory for run {self.run_number} already exists. Using old dir.')
        return
    
    def get_config_files(self):
        '''
        Get tar file from DAQ database.
        And open it to get the run config files
        '''
        urlGetTar = "http://dunedaq-microservices.cern.ch:5005/runregistry/getRunBlob/" + self.run_number
        if self.verbose >= 2:
            print(f"Exporting tar file of run {self.run_number} with run configurations from DAQ database.")
        subprocess.run(["curl","-u","fooUsr:barPass","-X","GET","-O","-J",urlGetTar], cwd=self.run_dir)
        # Get tar file name, since it changes depending on run, and untar it            
        Tar = os.listdir(self.run_dir)
        runTar = Tar[0]
        subprocess.run(["tar", "-xzf", runTar], cwd=self.run_dir)
        if self.verbose >=2:
            print("The tar file is: ", Tar[0])
        if Tar[0] == self.run_number:
            self.to_UconDB = False
            self.status_upload = True
            print(f'The configuration data of run {self.run_number} was not downloaded correctly from the DAQ DB. The info will not be uploaded to the UconDB.')
        return

    def get_meta_info(self):
        '''
        Get metadata using DAQ microservice.
        Create a list with all the files to use as headers in the blob
        '''
        urlGetMeta = "http://dunedaq-microservices.cern.ch:5005/runregistry/getRunMeta/" + self.run_number
        if self.verbose >= 2:
            print(f"Exporting metadatada or run {self.run_number} using daq microservice")
        subprocess.run(["curl","-u","fooUsr:barPass","-X","GET",urlGetMeta,"-o","runMeta.json"], cwd=self.run_dir)

        # Fill list with all name of config files
        listF = []
        for root, dirs, files in os.walk(self.run_dir):
            listF += [os.path.abspath(os.path.join(root,name)) for name in files]

        # Remove tar fiel and if boot and component files are in file list, move to 1st and 2nd position
        for name in listF:
            if '.tar.gz' in name:
                listF.remove(name)
            if 'runMeta,json' in name:
                listF.remove(name)
                listF.insert(0,name)
        return listF

    def write_blob(self):
        """
        Write the blob which will be send to the UConDB
        """
        if self.verbose >= 1:
            print(f'Writting blob with configurations data of run {self.run_number}.')
        bundle_daq = ''
        header = 'Start of Record\n Run Number: ' + self.run_number + '\n Packed on ' + time.strftime('%b %d %H:%M', time.gmtime()) + 'UTC\n'
        # Add file path and name with pound signs to see where files begin and find split points for unpacker
        for name in self.file_names:
            if name.endswith('.swp'):
                 print("removing swp file: ", name)
            else: # name ==" runMeta.json":
                bundle_daq += '\n#######\n' + name[name.rfind(self.run_dir):] + '\n#######\n'
                with open(name, 'r') as f:
                    bundle_daq += f.read()
        end = '\n End of Record \n RUn Number: ' + self.run_number + '\n Packed on: ' + time.strftime('%b %d %H:%M', time.gmtime()) + 'UTC\n'
        bundle = header + bundle_daq + end

        #Write bundle in a txt file so that we can send it with the curl command.
        with open(self.blob_str, 'w') as blob_file:
            blob_file.write(bundle)
            blob_file.close()
        return

    def blob_to_uconDB(self):
        '''
        Send the blob to the UConDB using curl so as not to install ucondb client
        '''
        if self.verbose >= 1:
            print(f'Transfering blob file of run {self.run_number} to UConDB... ')
        ret_code = subprocess.run(['curl','-T', self.blob_str,'--digest','-u','username:password','-X','PUT', self.ucondb_url])
        if ret_code.returncode == 7:
            print(f'Failed to connect to UConDB. Try source .db_startup.sh')
            self.remove_dir()
            exit(1) 
        elif ret_code != 0 and ret_code != 60:
            print('curl command returned with error code ' + str(ret_code) + '.')
        return

    def check_transf(self):
        '''
        Check if the blob was transfered succesfully to the UConDB
        '''
        if self.verbose >= 2:
            print(f"Check blob transfer of run {self.run_number} to UconDB, {self.ucondb_url}")
        t = 'curl ' + self.ucondb_url + ' > check_tr.txt'
        ret_code = subprocess.run(t, shell=True )
        with open('check_tr.txt') as f:
            head = [next(f) for x in range(1)]
            #head = [next(f) for x in range(10)]
        #if self.run_number in head[7]:
        if 'Start' in head[0]:
            if self.verbose >=2:
                print('The run was succesfully uploaded to the UconDB')
            return True
        else:
            if self.verbose >= 2:
                print(f'The run {self.run_number} was not transferred correctly to the UconDB.')
            return False
        return 

    def remove_dir(self):
        '''
        Remove run directory where the tar file from the DAQ db was saved, so as to save memory
        '''
        shutil.rmtree(self.run_dir)
        return

class configTransferInfo():
    '''
    This class gets the following info:
    The last runs uploaded to the DAQ DB (from the metadata obtained using the microservice of DAQ)
    The last run uploaded to the UconDB and missing runs (from a txt file that is updated with this scrip)
    '''
    def __init__(self, runinfo, runmeta, verbose = 2):
        '''
        :param runinfo: str
            the file name of the txt file where the info of the transfer to the UConDB is saved
        :param runmeta: str
            The file name of the txt file with the meta info of the lasts runs uploaded to the DAQ DB
        :param verbose: int (1,2,3), optional
            How much info do you want printed. 1 for less, 3 for more
        '''
        self.run_info = runinfo
        self.runs_meta = runmeta
        self.verbose = verbose

        self.meta_all_data = self.read_meta_file()
        self.meta_runs = self.get_meta_runs()

        self.runs_not_tr, self.last_run_tr  = self.read_run_transfer_info()
        if self.last_run_tr == self.meta_runs[0]:
            print('No new runs')
            exit(1)
        self.update_run_not_tr()
        return

    def read_meta_file(self):
        '''
        Read metadata for the last runs uploaded to the DAQ DB,
        and return the info as an array, without the line of value names
        '''
        if self.verbose >= 2:
            print(f"Reading the metadata info of the last runs uploaded to the UconDB")
        with open(self.runs_meta, 'r') as blob:
            meta = blob.read()
        databrack = re.findall(r"\[.*?\]", meta)
        data = []
        for line in databrack:
            if 'RUN_NUMBER' in line:
                meta_val = line
            elif '[[' in line:
                data.append(line[1:])
            else:
                data.append(line)
        if self.verbose >= 3:
            print(f'The data in the meta is: {data}')
        return data

    def get_meta_runs(self):
        '''
        Get the run number of the finished runs from the file with the metadata of the last #-runs
        '''
        runs_in_meta = []
        for meta in self.meta_all_data:
            data = meta.split(',')
            runs_in_meta.append(data[0][1:])
        runs = [int(i) for i in runs_in_meta]
        if self.verbose >= 2:
            print(f'The last runs uploaded to the DAQ DB are: {runs}')
        return runs

    def read_run_transfer_info(self):
        '''
        Read txt file with info of the transfer of the run config files to the UConBD
        get the last run uploaded to the UconDB and missing runs 
        '''
        if self.verbose >= 2:
            print(f'Retrieving the run transfer to the UconDB info')
        if os.stat(self.run_info).st_size == 0:
            print(f'File {self.run_info} is empty. Will work with meta file and fill run info file in the end. {self.meta_runs[-1]}')
            return [], self.meta_runs[-1]
        with open(self.run_info) as f:
            head = [next(f) for x in range(4)]
            not_transf = head[1].split(',')
        if '[]' in str(not_transf):
            not_tr = []
        else:
            not_tr = [int(i) for i in not_transf]
        last_tr = int(head[3])
        if self.verbose >= 2:
            print(f'The runs not transfered are: {not_tr}')
            print(f'The last run transferred is: {last_tr}')
        return not_tr, last_tr

    def update_run_not_tr(self):
        '''
        Update the runs that have not been transferred to the UConDB
        with the info from the metadata
        '''
        for run in range(int(self.last_run_tr)+1, int(self.meta_runs[0])+1):
            self.runs_not_tr.insert(0,run)
        if self.verbose >=2:
            print(f'The updated list of runs not transfered is: {self.runs_not_tr}')
        return


def update_upload_runs_ucon(info_file, info_class, upload_runs ):
    '''
    Update the txt file with the info of which runs have been uploaded to the UconDB
    and which runs have not.
    '''
    #rinfo = open(info_file, 'w'):
    head0 = 'Runs not transferred \n'
    head2 = '\nLast run transferred \n'
    if info_class.runs_not_tr:
        head1 = str(info_class.runs_not_tr[0]) 
        for run in range(1, len(info_class.runs_not_tr)):
            head1 +=', ' + str(info_class.runs_not_tr[run])
    else:
        head1 = '[]'
    if upload_runs:
        head3 = str(upload_runs[-1])
    else:
        head3 = str(info_class.last_run_tr)
    head = head0 + head1 + head2 + head3
    with open(info_file, 'w') as rinfo:
        rinfo.write(head)
    print(f'The run info file will be uploaded with the following info:\n  {head}')
    return

def write_in_log(log_file, info):
    with open(log_file, "a") as lfile:
        lfile.write(info)
    return 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Acces data from the DAQ DB and send it to the UConDB')
    parser.add_argument("--run", type=str, default=None, help="Run number to extract info")
    parser.add_argument("--runinfo", type=str, default="runsTransDone.txt", help="File with run info")
    parser.add_argument("--runmeta", type=str, default=None, help="File with run meta")
    args = parser.parse_args()
    run(args)


