from csv_diff import load_csv, compare
import os
import shutil

DIRNAME = './files'
ARCHIVE_DIRNAME = './archive'
FLUME_DIR = '../flume'


def make_dirs():
    if not os.path.exists(DIRNAME): os.makedirs(DIRNAME)
    if not os.path.exists(ARCHIVE_DIRNAME): os.makedirs(ARCHIVE_DIRNAME)
    if not os.path.exists(FLUME_DIR): os.makedirs(FLUME_DIR)


def is_different():
    print('Diffing files...\n')
    for filename in os.listdir(DIRNAME):
        if not os.path.exists(os.path.join(ARCHIVE_DIRNAME, filename)):
            return True
        print(filename)
        diff = compare(
            load_csv(open(os.path.join(DIRNAME, filename), encoding='utf8')),
            load_csv(open(os.path.join(ARCHIVE_DIRNAME, filename), encoding='utf8'))
        )
        print(diff)
        print()
        if len(diff['added']) > 0 or len(diff['removed']) > 0:
            return True

    return False


def archive_files():
    for filename in os.listdir(DIRNAME):
        shutil.copyfile(os.path.join(DIRNAME, filename), os.path.join(ARCHIVE_DIRNAME, filename))


def move_to_flume():
    for filename in os.listdir(FLUME_DIR):
        if filename.endswith('.COMPLETED'): os.remove(os.path.join(FLUME_DIR, filename))
    for filename in os.listdir(DIRNAME):
        shutil.copyfile(os.path.join(DIRNAME, filename), os.path.join(FLUME_DIR, filename))


def clear_files():
    for filename in os.listdir(DIRNAME):
        os.remove(os.path.join(DIRNAME, filename))


def run_comparison():
    if is_different():
        print('Difference detected')
        print('Archiving files...')
        archive_files()
        print('Moving to flume..')
        move_to_flume()

    print('Removing files...')
    clear_files()
