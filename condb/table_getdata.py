# Usage python table_getdata.py --run run_num 

from condb import ConDBClient
import argparse

def run(args):
    '''
    Main run method
    The table name must be probided, here an example table name is shown
    '''
    
    table = "pdunesp.test" #Name of table in db schema_name.table_name
    url = "https://dbdata0vm.fnal.gov:9443/dune_runcon_prod"
    db = connect_db(url)
    run = 23331 # Example of a run number: 23330
    if args.run:
        run = args.run

    # get_data example - Get data from given run
    db.get_data(table, t0=run)

    # search_data example - Search data, or runs that comply with the following conditions
    con = [("run_type","=",'PROD'),("buffer",">=",0)] # Example conditions on the data
    columns, data = db.search_data(table, conditions=con)
    print("columns:", ','.join(columns))
    for line in data:
        print(line)
    return

def connect_db(url):
    '''
    Connect to the condb db
    The url must be provided, 
    The username and password are necessary to put data into the db but NOT for reading
    Contact Ana Paula Vizcaya or Norm Buchanan to get the generic ProtoDUNE username 
    '''
    #user = ""
    #passw = ""
    db = ConDBClient(url) #, username=user, password=passw)
    return db


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Look at condb with python API')
    parser.add_argument("--run", type=int, default=None, help="Run number to extract info")
    args = parser.parse_args()
    run(args)
