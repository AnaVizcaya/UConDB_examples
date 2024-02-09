# Usage python table_getdata.py --run run_num 

from condb import ConDB
import argparse

def run(args):
    '''
    Main run method
    '''

    db = connect_db()
    table = "pdunesp.test" #Name of table in db schema_name.table_name
    run = 23331 # Example of a run number: 23330
    if args.run:
        run = args.run

    # get_data example
    db.get_data(table, t0=run)

    # search_data example
    con = [("run_type","=",'PROD'),("buffer",">=",0)] # Example conditions on the data
    columns, data = db.search_data(table, conditions=con)
    print("columns:", ','.join(columns))
    for line in data:
        print(line)
    return

def connect_db():
    '''
    Connect to the condb db
    The username and password are necessary to put data into the db
    '''
    url = "https://dbdata0vm.fnal.gov:9443/dune_runcon_prod"
    user = ""
    passw = ""
    db = ConDBClient(url, username=user, password=passw)
    return db


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Look at condb with python API')
    parser.add_argument("--run", type=int, default=None, help="Run number to extract info")
    args = parser.parse_args()
    run(args)
