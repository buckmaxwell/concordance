from constants import *
import sqlite3


def read_in_chunks(file_object, chunk_size=CHUNK_SIZE):
    """Generator to read a file piece by piece."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def get_connection(file_no=1, db_name=None):
    """Get a connection to the database"""

    if not db_name: # TODO: does this follow DRY?
        db_name = 'bridgewater{}.db'.format(file_no)

    conn = sqlite3.connect(db_name)
    return conn
