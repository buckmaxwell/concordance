#!/usr/bin/env python3.6

from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
from helpers import read_in_chunks, get_connection
from uuid import uuid4
from constants import *
import requests
import time
import pika
import sys
import sqlite3
import spacy
import logging
import math
import os
import json



logging.basicConfig(level=logging.INFO)
pool = ThreadPool(NUMBER_OF_THREADS) # create threadpool

def get_database_names():
    """Return a list of database names"""
    result = []
    for thread_no in range(0, NUMBER_OF_THREADS):
        result.append('bridgewater{}.db'.format(thread_no + 1))
    return sorted(result)


def create_words_tables(databases):
    """Create the table to store the words"""
    for db_name in databases:
        connection = get_connection(db_name=db_name)
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS words( word TEXT, sentence_no INTEGER, file_no INTEGER);''')
        cursor.execute('''DELETE FROM words''')
        cursor.execute('''DROP INDEX IF EXISTS word_index''')
        connection.commit()
        connection.close()
    return True


def combine_database_files(databases):
    """
    Combine all the database files into one database, return name of main database
    """
    sorted(databases)
    conn = get_connection(db_name=databases[0])
    cursor = conn.cursor()
    for db in databases[1:]:
        

        # get current db count
        cursor.execute('''SELECT MAX(sentence_no) from words''')
        sentences_so_far = cursor.fetchone()[0]

        # update sentence count in current db
        temp_conn = get_connection(db_name=db)
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute('''UPDATE words SET sentence_no = sentence_no + ?''', [sentences_so_far])
        temp_conn.commit()
        temp_conn.close()

        # attach next db to main db
        db2_name = 'db' + str(uuid4()).replace('-','')[:6]
        cursor.execute('''ATTACH ? AS ?;''', [db, db2_name])
        cursor.execute('INSERT INTO main.words SELECT * FROM {}.words;'.format(db2_name) )
        conn.commit()

    conn.close()

    return databases[0]
    


def add_index_to_db(cursor):
    '''Add index on word column to speed up alphabetical retrieval'''
    cursor.execute('''CREATE INDEX word_index on words (word);''') 
    return True


def integer_to_letter(integer):
    """Solution for properly numbering a file alphabetically"""

    alph = 'abcdefghijklmnopqrstuvwxyz'
    multiplier = math.ceil(integer / 26.0)
    letter = integer - (multiplier-1)*26
    letter = alph[letter - 1] * multiplier 
    spaces = (6 - len(letter)) * ' '
    return '{}.{}'.format(letter, spaces)


def print_db(cursor, outputfile=None):
    """Prints database contents according to problem specs"""

    last_word = None
    occurrence_list = []
    line_no = 0
    if outputfile:
        f = open(outputfile, 'w')

    for row in cursor.execute('''SELECT word,sentence_no,file_no FROM words ORDER BY word;'''):

        if last_word and (row[0] != last_word): # print when the word switches (last word is fake word)
            line_no += 1
            spaces = (25 - len(last_word)) * ' '
            count_and_list = '{' + str(len(occurrence_list)) + ':' + ','.join(occurrence_list) + '}'
            letter = integer_to_letter(line_no)
            if outputfile:
                f.write("{letter}{word}{spaces}{count_and_list}\n".format(letter=letter, word=last_word,
                spaces=spaces, count_and_list=count_and_list))

            else:
                print("{letter}{word}{spaces}{count_and_list}".format(letter=letter, word=last_word,
                spaces=spaces, count_and_list=count_and_list))
            # free occurences list
            del occurrence_list[:] 

        last_word = row[0]
        # append sentence_no to occurrences list
        occurrence_list.append(str(row[1]))
    
    if outputfile:
        f.close()

    return True


def split_file(filename):
    """
    Split a file into a few files and return the new filenames
    """
    # open a file for each thread
    open_files = []
    fid = 1
    for x in range(0, NUMBER_OF_THREADS):
        f = open('___generated___.%d' %fid, 'w')
        open_files.append(f)
        fid += 1

    filesize = os.path.getsize(filename)
    new_filesize = int(math.ceil(filesize / float(NUMBER_OF_THREADS)))

    with open(filename, 'r') as f:
        # final sentence may not be a complete sentence, save and prepend to next chunk
        leftovers = ''
        sentence_no = 0
        current_file = 0
        # store batch word inserts here
        word_rows = []
        for chunk in read_in_chunks(f): # lazy way of reading our file in case it's large

            # prepend leftovers to chunk
            chunk = leftovers + chunk

            # current file is too large
            if os.path.getsize(open_files[current_file].name) > new_filesize:
                doc = nlp(chunk)
                sents = [sent.string.strip() for sent in doc.sents]
                leftovers = sents[-1]
                open_files[current_file].write(' '.join(sents[:-1]))
                current_file += 1
            else: # middle of file
                open_files[current_file].write(chunk)

    # close all open files
    new_filenames = []
    for f in open_files:
        new_filenames.append(f.name)
        f.close()

    return new_filenames



if __name__ == '__main__':

    # start timer
    start_time = datetime.now()

    if len(sys.argv) < 2:
        print("Please provide the name of a text file containing \
            an English text document.\n\n./concordance myfile.txt")
        exit() # exit program
    else:
        filename = sys.argv[1]
        outputfile = None
        if len(sys.argv) > 2:
            outputfile = sys.argv[2]

    logging.info('Loading NLP...')
    nlp = spacy.load('en') # takes a while, handle possible error first
    database_names = get_database_names()

    logging.info('Preparing database...')
    create_words_tables(database_names)

    logging.info('Splitting files for multithreading...')
    filenames = split_file(filename)

    logging.info('Adding data to database...')
    # use message passing so that different workers can share work

    # publish filenames to filename queue
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    q = channel.queue_declare(queue='filename_queue')
    for fn in filenames:
        channel.basic_publish(exchange='', routing_key='filename_queue', body=fn)

    
    # Unacknowledged message count?
    completed = 0
    sentences = []
    q = channel.queue_declare(queue='completed')
    while completed < len(filenames):
        method_frame, header_frame, body = channel.basic_get('completed')
        if method_frame:
            completed += 1
            sentences.append(int(body))
            channel.basic_ack(method_frame.delivery_tag)
        else:
            pass # no message returned
            time.sleep(2)



    logging.info('Combining database files...')
    main_db_name = combine_database_files(database_names)

    # combine databases
    logging.info('Indexing database...')
    conn = get_connection(db_name=main_db_name)
    cursor = conn.cursor()
    add_index_to_db(cursor)
    conn.commit() # final commit

    if outputfile:
        print_db(cursor, outputfile)
    else:
        print_db(cursor)

    conn.close() # close db connection

    total_time = round((datetime.now() - start_time).total_seconds(), 2)
    print('success!!\n\nFinished in {} seconds.'.format(total_time))

