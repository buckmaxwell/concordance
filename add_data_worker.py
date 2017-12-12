from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
from constants import *
from uuid import uuid4
import pika
import sys
import sqlite3
import spacy
import logging
import math
import os
from helpers import read_in_chunks, get_connection


nlp = spacy.load('en') # takes a while, handle possible error first

def insert_word_rows(connection, word_rows):
    """
    Insert words into database in a bulk insert.

    word_rows = [(word, sentence_no, file_no), (word, sentence_no, file_no), ...]

    """
    connection.executemany('''INSERT INTO words VALUES (?,?,?)''', word_rows)
    connection.commit()
    return True


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


def add_data_to_db(filename, file_no):
    """
    Read data from file and populate words table - returns number of sentences analyzed
    """
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


def handle_message(ch, method, properties, body):
        filename = body.decode("utf-8") 
        print(filename)
        sentence_no = add_data_to_db(filename, int(filename.split('.')[-1]) )
        print('done')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        q = channel.queue_declare(queue='completed')
        channel.basic_publish(exchange='', routing_key='completed', body=str(sentence_no))


if __name__ == '__main__':

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='filename_queue')
    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(handle_message, queue='filename_queue', no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()












