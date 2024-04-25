from condb import ConDB
import argparse
import time


def run(args):
    '''
    Main run method
    '''
    conDBRuns()

    return


class conDBRuns():
    '''
    Gets the last runs uploaded to the conditions database (conDB) 
    '''

    def  __init__(self, verbose = 2, run = None, data = None): #, detector = None):
        '''
        :param verbose: int (1,2,3), optional
            how much info do you want printed. 1 for less, 3 for more
        '''
        self.verbose = verbose
        self.run = run
        self.data = data
        #self.detector = detector

        self.folder = self.connect()
        #tags = self.folder.tags()
        #for tag in tags:
        #    print(tag, 'This is the tag')

        #self.folder.tag('v1.2', comment = 'update the buffer 2')
        self.data_since = self.data_dem()
        self.data_con()
        self.data_put()
        return
    def connect(self):
        host    = 'ifdbprod2.fnal.gov'
        port    = '5451'
        db_name = 'pdunesp_prod'
        user    = '#####'
        passw =   '######'
        connstr = f'host={host} port={port} dbname={db_name} user={user} password={passw}'
        db = ConDB(connstr = connstr)
        return db.openFolder('pdunesp.test')   #('pdunesp.run_conditionstest')  #('mcconfig.testsearch') #('pdunesp.test') pdunesp.test_prueba

    def data_con(self):
        con = [] #[('run_type', '=', 'PROD'), ('stop_time', '=', )]                            # [('electron_lifetime', '=', 9)] #("campaign", '=', "fd_mc_2023a"),
        #data = self.folder.searchData(tag = 'v1.2', conditions=con, tr = '1699634862')  #data_type="np04_hd")
        data = self.folder.getData(0,t1=25943) #22739, data_type="np04_hd", tag='v1.1')
        #self.runs = [int(row[1]) for row in data]
        for row in data:
            print(f'{row}')
            print(f'run number {row[1]}, and rest of columns {row[4:]} ')
        return data
    def data_dem(self):
        con = [('run_type', '=', 'TEST')]
        data = self.folder.searchData(conditions=con)#, data_type="np04_hd", tag='v1.1')
        for row in data:
            print(f'Esta {row}')
        return data

    def data_put(self):
        print("addindg data")
        chunk = []
        chunk.append((0,23300.0,1700067406.9728901,"np02_coldbox",1700067406.9728646,1700067803.0,"None","TEST","aaaaaa",None,None))
        self.folder.addData(chunk)
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Access conditions data from the UconDB and send to run config DB')
    args = parser.parse_args()
    run(args)
