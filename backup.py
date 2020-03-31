from subprocess import check_output, Popen, PIPE
from datetime import datetime, timedelta
import gzip
import os
import string
import random
import re

backup_path = '/home/dameko/Documents/monetdb-backup/'
dbfarm_path = '/home/dameko/Documents/monetdb-s3/s3dbfarm'
days_gap = 1

if backup_path[-1] != '/':
    backup_path = backup_path + '/'

if dbfarm_path[-1] != '/':
    dbfarm_path = dbfarm_path + '/'


def random_string(N=6):
    """Generate a random string of fixed length """
    rand_str = (''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                        for _ in range(N)))
    return rand_str


def get_db_names(folder_path):
    try:
        return [f.name for f in os.scandir(folder_path) if f.is_dir()]
    except FileNotFoundError:
        print("Incorrect path to the dbfarm.")
        exit(1)


def get_file_names(folder_path):
    try:
        return [f.name for f in os.scandir(folder_path) if f.is_file()]
    except FileNotFoundError:
        raise FileNotFoundError


def create_backup(backup_file, dbname):

    with open(backup_file, "a") as outfile:
        backup = Popen(["msqldump", "-N", "-d", dbname], stdout=PIPE)
        Popen(["gzip"], stdin=backup.stdout, stdout=outfile)
        backup.stdout.close()

    # Add timestamp when the backup is finished
    with gzip.open(backup_file, 'a') as file:
        file.write(('-- ' + datetime.now().strftime('%d-%m-%Y %H:%M:%S') + '\n').encode())
        file.close()


def find_latest_backup(backup_path, db_name):
    backups_dict = {}

    for backup_file in get_file_names(backup_path):
        if re.search(r'(.*_)[A-Z 0-9]{6}(_backup.sql.gz)$', backup_file):
            try:
                with gzip.open(backup_file, 'r') as file:
                    # Read the timestamp from the first line
                    first_data = file.peek(10).decode("utf-8")
                    timestamp = datetime.strptime(first_data, '-- %d-%m-%Y %H:%M:%S\n')
                    backups_dict[backup_file] = timestamp
                    print(timestamp)
                    file.close()
            except ValueError:
                print(backup_file + " may be corrupted, no timestamp found.")
            except FileNotFoundError:
                pass

    if not backups_dict:
        return None

    return max(backups_dict.values(),
               key=lambda v: v if isinstance(v, datetime) else datetime.max)


db_names = get_db_names(dbfarm_path)

for name in db_names:

    # Check if the credentials are correct
    cmd = check_output(["mclient", "-d", name, "--statement", "SELECT 'True';", "--format", "csv"])
    if 'True' not in cmd.decode("utf-8"):
        print("Couldn't connect to the database {0}. Check your credentials.".format(name))
        continue

    # Check timestamps of backups, decide if a backup is needed
    latest_backup_tmsp = find_latest_backup(backup_path, name)
    print('Latest', name, '--', latest_backup_tmsp)
    if latest_backup_tmsp:
        delta = datetime.now() - timedelta(hours=24 * days_gap)
        if latest_backup_tmsp < delta:
            backup_file = backup_path + name + '_' + random_string() + '_backup.sql.gz'
            create_backup(backup_file, name)

    else:
        backup_file = backup_path + name + '_' + random_string() + '_backup.sql.gz'
        create_backup(backup_file, name)
