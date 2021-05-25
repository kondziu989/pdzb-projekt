import pandas as szpark
import os
import transform_dates
from run_cmd import run_cmd
from datetime import datetime
from dateutil.relativedelta import relativedelta


OUTPUT_DIR = 'digested'
INPUT_DIR = 'files'
HDFS_CSV_DIR = 'hdfs://user/cloudera/flume/events'
IMPALA_DIR = '/user/hive/warehouse/f1.db'

STATUS_COLUMNS = ["statusId", "status"]
DRIVERS_COLUMNS = ["driverId", "forename", "surname", "nationality", "dob"]
CIRCUITS_COLUMNS = ["circuitId", "name", "country"]
RACES_COLUMNS = ["raceId", "name", "date"]
CONSTRUCTOR_COLUMNS = ['constructorId', 'name', 'nationality']

CONSTRUCTORS_TABLE_COLUMNS = {
    "constructorId": "ConstructorId",
    "name": "ConstructorName",
    "nationality": "Nationality"
}

STATUS_TABLE_COLUMNS = {
    "statusId": "StatusId",
    "status": "StatusType"
}

DRIVERS_TABLE_COLUMN = {
    "driverId": "DriverId",
    "forename": "FirstName",
    "surname": "LastName",
    "dob": "DateOfBirth"
}

CIRCUITS_TABLE_COLUMN = {
    "circuidId": "CircuitId",
    "name": "CircuitName",
    "country": "Country"
}

RACES_TABLE_COLUMN = {
    "raceId": "RaceId",
    "name": "RaceName",
    "date": "RaceDate"
}

mapper = {
    'status.csv': [STATUS_COLUMNS, STATUS_TABLE_COLUMNS, 'status'],
    'drivers.csv': [DRIVERS_COLUMNS, DRIVERS_TABLE_COLUMN, 'driver'],
    'circuits.csv': [CIRCUITS_COLUMNS, CIRCUITS_TABLE_COLUMN, 'circuit'],
    'races.csv': [RACES_COLUMNS, RACES_TABLE_COLUMN, 'race'],
    'constructors.csv': [CONSTRUCTOR_COLUMNS, CONSTRUCTORS_TABLE_COLUMNS, 'constructor']
}


def load_file(csv_name):
    return szpark.read_csv(os.path.join('archive', csv_name))


def select_columns(columns, frame):
    to_delete = list(set(frame.columns) - set(columns))
    return frame.drop(to_delete, 'columns')


def write_file(df, name):
    df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)


def digest(df, field_dict):
    return df.rename(columns=field_dict)


def send_to_hdfs():
    for filename in os.listdir(OUTPUT_DIR):
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', os.path.abspath(os.path.join(OUTPUT_DIR, filename)),
                                   os.path.join(IMPALA_DIR, mapper[filename][2])])
        print(ret, out, err)

def get_races_date_dims():
    df = load_file('races.csv')
    result_strings = [transform_dates.get_date_dims(x, y) for x, y in zip(df['date'], df['time'])]
    result = [s.split(';') for s in result_strings]
    result_df = szpark.DataFrame(result, columns=['raceDate', 'Year', 'semester', 'quarter', 'Month'])
    print('Writing file', 'racedate.csv')
    write_file(result_df, 'racedate.csv')

def write_files():
    for file in mapper:
        frame = load_file(file)
        frame = select_columns(mapper[file][0], frame)
        frame = digest(frame, mapper[file][1])
        print('Writing file', file)
        write_file(frame, file)
    get_races_date_dims()
    send_to_hdfs()


PARTICIPATION_COLUMNS = ['driverid', 'raceid', 'constructorid', 'points', 'position', 'startingposition',
                         'pitstopnumber', 'avgpitstopduration', 'avglaptime', 'driverage', 'numberofraces', 'statusid',
                         'circuitid']

SELECTED_PARTICIPATION_COLUMNS = [
    'resultId',
    'raceId',
    'driverId',
    'constructorId',
    'grid',
    'position',
    'points',
    'statusId'
]

PARTICIPATION_MAPPER = {
    'grid': 'startingposition'
}

def create_fact():
    df = szpark.DataFrame(columns=PARTICIPATION_COLUMNS)
    results = load_file('results.csv')
    drivers = load_file('drivers.csv')
    races = load_file('races.csv')
    constructors = load_file('constructors.csv')
    pit_stops = load_file('pit_stops.csv')
    laps = load_file('lap_times.csv')

    pit_stops = pit_stops.groupby(['raceId', 'driverId']).agg({'stop': 'max', 'milliseconds': 'mean'}).rename(columns={'stop': 'numberofpitstops', 'milliseconds': 'avgpitstopduration'}).reset_index()
    results = select_columns(SELECTED_PARTICIPATION_COLUMNS, results)
    results = digest(results, PARTICIPATION_MAPPER)
    results = szpark.merge(results, pit_stops, on=['raceId', 'driverId'])

    laps = laps.groupby(['raceId', 'driverId']).agg({'milliseconds': 'mean'}).rename(columns={'milliseconds': 'avglaptime'}).reset_index()
    results = szpark.merge(results, laps, on=['raceId', 'driverId'])
    write_file(results, 'participation.csv')

    # print(results)
    for index, result in results.iterrows():
        race = races.loc[races['raceId'] == result['raceId']].to_dict('records')[0]
        driver = drivers.loc[drivers['driverId'] == result['driverId']].to_dict('records')[0]
        constructor = constructors.loc[constructors['constructorId'] == result['constructorId']].to_dict('records')[0]
        driver_age = relativedelta(datetime.strptime(race['date'], '%Y-%m-%d'), datetime.strptime(driver['dob'], '%Y-%m-%d')).years
        print(driver_age)
        # driverAge = date.strftime() driver['dob']

        # print(race)

create_fact()
