from pyspark.sql import SparkSession
from compare_files import run_cmd
import os
from pyspark.sql import SQLContext
spark = SQLContext(sc)

HDFS_CSV_DIR = 'hdfs://user/cloudera/flume/events'
# spark = SparkSession.builder.getOrCreate()

STATUS_COLUMNS = ["status"]
DRIVERS_COLUMNS = ["forename", "surname", "nationality","dob"]
CIRCUITS_COLUMNS = ["name", "country"]
RACES_COLUMNS = ["name",  "date"]
CONSTRUCTOR_COLUMNS = ['constructorId', 'name', 'nationality']

mapper = {
    'status.csv': STATUS_COLUMNS,
    'drivers.csv': DRIVERS_COLUMNS,
    'circuits.csv': CIRCUITS_COLUMNS,
    'races.csv': RACES_COLUMNS,
    'constructors.csv': CONSTRUCTOR_COLUMNS
}


def load_file(csv_name):
    return spark.read.csv(HDFS_CSV_DIR + '/' + csv_name)


def select_columns(columns, frame):
    to_delete = list(set(frame.columns) - set(columns))
    return frame.drop(to_delete, 'columns')


def write_file(df, name):
    df.write.mode('overwrite').option('header', 'true').csv('hdfs://temp/{}.csv'.format(name))


# def push_to_impala(frame):
#     (ret, out, err) = run_cmd(['hdfs', 'dfs', '-copyFromLocal', '-f', os.path.abspath(os.path.join(DIRNAME, filename)), os.path.join(HDFS_DIR, filename)])


def get_dims():
    res = []
    for file in mapper:
        print(file)
        frame = load_file(file)
        frame = select_columns(mapper[file], frame)
        write_file(frame, file)
        res.append(frame)
    return res


print('Running spark...')
x = get_dims()
print(x)
