#!/usr/bin/env python3.6


# sort words alphabetically, keeping track of sentence and number of occurrences

# using a binary search tree, we could store an alphabetical list that stays in 
# alphabetical order. We'd have to balance it occasionally (maybe use red and black
# trees) on each of the nodes we could store a word, the number of occurrences,
# and a list of places it occurs (sentence numbers).

# advantages
#  - speedy printing in alphabetical order
#  - low-ish memory footprint
#  - relatively fast time to add 

# the data structure we need is called a TreeMap in java
# - implements red and black trees
# - stores inputs in sorted order
# - maps values to keys -- our key will be a list of sentence numbers [1,1,2]


# OrderedDict (remembers order added); like LinkedHashMap in java
# Can be used in  conjunction with sorting to make a sorted dictionary
# OrderedDict(sorted(d.items(), key=lambda t: t[0]))

# Problem? Memory footprint could be huge for large inputs
# storing all encountered words in memory could crash the computer if input size is 
# arbitrarilty large.  Storing in database could work, but has the overhead of 
# needing a database.  SQLite provides a solution that can be packaged with our program
# and still can provide fast times for row adds, lookups, and updates to our concordance.
# an order by clause can spit out our data in alphabetical order.

# If we keep an index on the key, the order by clause will be very quick, however,
# starting with an index on the word/key will significantly slow down writes to our 
# database.  To speed up writes, we could not index until the end, allowing us to spit the
# words back out quickly....

# Problem is, if we need to update a words count and our values are not indexed, a lookup 
# will be very expensive O(N) time! A full table scan.

# Instead, it is sensible to write lines with no indexes that look like

# |     word      |   sentence_no   |
# | flabbergasted | 3               |
# |     good      | 3               |
# | flabbergasted | 4               |
# |     jim       | 4               |

# We can do one more thing to optimize write speed, and that is bulk transactions
# 
# From the SQLite optimization FAQ: 
#
# `Unless already in a transaction, each SQL statement has a new transaction started
# for it. This is very expensive, since it requires reopening, writing to, and closing the
# journal file for each statement. This can be avoided by wrapping sequences of SQL
# statements with BEGIN TRANSACTION; and END TRANSACTION; statements. This speedup is also
# obtained for statements which don't alter the database.`

# NOTES

#  - This program considers contractions to be multiple words (meaningwise, they are).
#    different tokenizers handle this problem differently.  We use spaCy, a common 
#    alternative is NLTK.  Both solutions could work, but we choose spaCy because it's
#    much speedier at tokenizing words (our primary use case).
#    
#    [graphic](timing.png)
# 
#     Contractions could be handled differently by providing special cases, a built-in
#     feature of spaCy.

from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool 
from uuid import uuid4
import sys
import sqlite3
import spacy
import logging
import math
import os

# CONSTANTS
BULK_INSERT_SIZE = 2500 # (rows)
NUMBER_OF_THREADS = 4 # max 10
CHUNK_SIZE = 1024

logging.basicConfig(level=logging.DEBUG)
pool = ThreadPool(NUMBER_OF_THREADS) # create threadpool
#results = pool.map(add_word, nlp(sentence))

def get_database_names():
    """Return a list of database names"""
    result = []
    for thread_no in range(0, NUMBER_OF_THREADS):
        result.append('bridgewater{}.db'.format(thread_no + 1))
    return sorted(result)


def get_connection(file_no=1, db_name=None):
    """Get a connection to the database"""

    if not db_name: # TODO: does this follow DRY?
        db_name = 'bridgewater{}.db'.format(file_no)

    conn = sqlite3.connect(db_name)
    return conn


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


def insert_word_rows(connection, word_rows):
    """
    Insert words into database in a bulk insert.

    word_rows = [(word, sentence_no, file_no), (word, sentence_no, file_no), ...]

    """
    connection.executemany('''INSERT INTO words VALUES (?,?,?)''', word_rows)
    connection.commit()
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
        cursor.execute('''SELECT count(*) from words''')
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


def read_in_chunks(file_object, chunk_size=CHUNK_SIZE):
    """Generator to read a file piece by piece."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def add_sentence_to_word_rows(sentence, word_rows, sentence_no, file_no):

    for token in nlp(sentence):
        # make sure token is not punctuation
        if (len(token) == 1 and token.pos_ == 'PUNCT') or (len(token) == 3 and token.pos_ == 'PUNCT'):
            continue
        else: # token is word
            # standard case the word and remove whitespace
            word = str(token).lower().strip()
            # add the word to batch insert list (if word is not the empty string)
            if word:
                word_rows.append([word, sentence_no, file_no])

    return True


def add_data_to_db(filename_and_file_no):
    """
    kwargs are filename and file_no
    Read data from file and populate words table - returns number of sentences analyzed
    """
    filename, file_no = filename_and_file_no
    connection = get_connection(file_no=file_no)
    
    with open(filename, 'r') as f:

        # final sentence may not be a complete sentence, save and prepend to next chunk
        leftovers = ''
        sentence_no = 0
        # store batch word inserts here
        word_rows = []

        for chunk in read_in_chunks(f): # lazy way of reading our file in case it's large

            # prepend leftovers to chunk
            chunk = leftovers + chunk

            # run nlp
            chunk_doc = nlp(chunk)
            # tokenized sentences from chunk
            sents = [sent.string.strip() for sent in chunk_doc.sents]

            # save for next chunk
            leftovers = sents[-1]

            for s in sents[:-1]:
                sentence_no += 1
                # add sentence to db
                add_sentence_to_word_rows(s, word_rows, sentence_no, file_no)
            
            if len(word_rows) >= BULK_INSERT_SIZE:
                insert_word_rows(connection, word_rows)
                del word_rows[:] # saves us the pain of reassigning and waiting for a garbage collector


    if word_rows: # insert leftovers into word rows, then left over word rows into db
        sentence_no += 1
        add_sentence_to_word_rows(leftovers, word_rows, sentence_no, file_no)
        insert_word_rows(connection, word_rows)


    # insert final 'fake' word row # TODO: this is odd
    insert_word_rows(connection, [['z'*25, 0, file_no]])

    connection.commit()
    connection.close()
    return sentence_no

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
    filenames_to_sentences = {}
    for fn in filenames:
        filenames_to_sentences[fn] = None


    logging.info('Adding data to database...')
    file_info_arrays = [(fn, int(fn.split('.')[-1])) for fn in filenames]
    sentences = pool.map(add_data_to_db, file_info_arrays)

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
    print('success!!\n\nAnalyzed {} sentences in {} seconds.'.format(sum(sentences), total_time))

