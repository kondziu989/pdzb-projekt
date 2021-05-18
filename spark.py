import pandas as spark
import os
from run_cmd import run_cmd

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
    return spark.read_csv(os.path.join(INPUT_DIR, csv_name))


def select_columns(columns, frame):
    to_delete = list(set(frame.columns) - set(columns))
    return frame.drop(to_delete, 'columns')


def write_file(df, name):
    df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)


def digest(df, field_dict):
    return df.rename(columns=field_dict)


def send_to_hdfs():
    for filename in os.listdir(OUTPUT_DIR):
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', os.path.abspath(os.path.join(OUTPUT_DIR, filename)), os.path.join(IMPALA_DIR, mapper[filename][2])])
        print(ret, out, err)


def write_files():
    for file in mapper:
        frame = load_file(file)
        frame = select_columns(mapper[file][0], frame)
        frame = digest(frame, mapper[file][1])
        print('Writing file', file)
        write_file(frame, file)
        send_to_hdfs()

