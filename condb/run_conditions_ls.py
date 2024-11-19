from condb2 import ConDBClient
import argparse
from datetime import datetime, timezone
import time
import pandas as pd

def run(args):
    #main run method

    #Set the correct url to the database
    client = ConDBClient('https://dbdata0vm.fnal.gov:9443/dune_runcon_prod')
    folder = 'pdunesp.run_conditionstest'

    #get the cuts
    #Cut on the date when the run was taken
    t0 = '2024-07-12 15:00:00' #(%Y-%m-%d %H:%M:%S)
    t1 = '2024-10-15 00:00:00' #(%Y-%m-%d %H:%M:%S)
    con1 = ('start_time', '>', utc_to_unix(t0))
    con2 = ('start_time', '<', utc_to_unix(t1))
    #Cust on the other columns
    con3 = ('beam_momentum','<','-0')
    con = [con1, con2]

    "Get the data"
    col, gen = client.search_data(folder, conditions=con) 
    print("the data is the following:")
    #print(col[1], col[29])
    lst = []
    for line in gen:
        #print(line[1], line[29])
        lst.append(line)
    df = pd.DataFrame(lst, columns = col)
    df.rename(columns={'tv': 'run_number'}, inplace=True)
    # Select columns that contain 'time' in their name
    cols_time = [col for col in df.columns if 'time' in col]
    for col_t in cols_time:
        df[col_t] = df[col_t].apply(unix_to_utc)
    df['beam_momentum'] = df['beam_momentum'].apply(fix_beam_momentum_neg)

    #save to csv
    if args.tocsv:
        print('saving a csv copy of the run conditions')
        file_name = args.tocsv + '.csv'
        df.to_csv(file_name, index=False)
    return

def fix_beam_momentum_neg(momentum):
    'Negative beam momentum is not rounded, it should be fixed in version 1.2 of the run conditions db'
    if momentum == 'None':
        return 'None'
    if momentum >= 0.8 or momentum <= -0.8:
        momentum = round(momentum,0)
    else:
        momentum = round(momentum,1)
    return momentum

def utc_to_unix(utc_time):
    """
    Convert UTC human-readable format to Unix timestamp.
    """
    dt = datetime.strptime(utc_time, '%Y-%m-%d %H:%M:%S')
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def unix_to_utc(unix_time):
    """
    Convert Unix timestamp to UTC human-readable format.
    """
    if unix_time == 'None':
        return 'None'
    utc_time = datetime.fromtimestamp(unix_time, tz=timezone.utc)
    return utc_time.strftime('%Y-%m-%d %H:%M:%S %Z')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Access run conditions data from the run conditions database')
    parser.add_argument("--run", type=int, default=None, help="Run number to extract info")
    parser.add_argument("--tocsv", type=str, default='', help="Give file name to save the run conditions info")
    args = parser.parse_args()
    run(args)
