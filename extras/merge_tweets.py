"""
This script

* Was used to do two things:
    - Take all the tweets saved in file and transfer to Mongo
    - Take old collections in Mongo and 'merge' in to one

Author: Muhamad Noor Zainal MUHAMAD ZABIDI
Python 3.5
"""
import json

import utils as bf

from tweet_simplify import simplify
from pyprind import ProgBar

__version__ = '2.0'

"""
--------------------------------------------------------------------
                          File Functions
--------------------------------------------------------------------
"""


def del_empty():
    """
        Deletes any emtpy entries in the list (stray quotation marks)
        and then adds a quotation mark to the end of each string so that
        it becomes a valid string JSON
            - prevent unterminated string JSON errors
            - removes empty entries so that when simplifying the tweet
                there are no dictionary related errors
    """

    delete_list = []

    for i in range(0, len(split_data)):
        if len(split_data[i]) is 0:
            delete_list.append(i)

    if len(delete_list) is not 0:
        delete_list.sort(reverse=True)

        for i in delete_list:
            del split_data[i]

    for i in range(0, len(split_data)):
        if split_data[i][:] is not r'"':
            split_data[i] += r'"'


def read_file(file_name):
    """
        Read a file and store all the data to a string
        then splits all the data with the criteria '\r\n' as each tweet ends
        with that and so it is easier to identify
    """

    split_criteria = r'\r\n"'

    with open(file_name, 'r') as fr:
        all_data = fr.read()

    return all_data.split(split_criteria)


"""
--------------------------------------------------------------------
                              MAIN
--------------------------------------------------------------------
"""

if __name__ == '__main__':

    db = bf.connect_db()
    main_collection = bf.connect_collection(db)

    """
    --------------------------------------------------------------------
                             File Operations
    --------------------------------------------------------------------
    """

    file_directory = "old_data/"
    file_names = ["2015-10-11.json", "2015-10-12 (2).json",
                  "2015-10-12.json", "2015-10-18.json",
                  "2015-10-182.json", "2015-11-09.json",
                  "2015-11-10.json"]

    # start_time = bf.start_timer()
    bar = ProgBar(len(file_names), monitor=True,
                  title='Transferring from file to MongoDB')
    for file in file_names:
        split_data = read_file(file_directory + file)
        del_empty()

        for line in split_data:
            try:
                tw = simplify(json.loads(json.loads(line)))
                main_collection.insert(tw)
                # counter += 1
            except KeyError:
                continue
        bar.update()
    print(bar)

    """
    --------------------------------------------------------------------
                             Database Operations
    --------------------------------------------------------------------
    """

    collection_names = ['firsthalf', 'million2',
                        'new_format']
    col_file = 'old_data/'

    bar = ProgBar(len(collection_names), monitor=True,
                  title='Writing to file from MongoDB')

    for col in collection_names:
        tw_cursor = db[col].find(projection={'_id': False})

        with open(col_file + col + '.json', 'a+') as f:
            for tweet in tw_cursor:
                f.write(json.dumps(tweet) + '\n')

        bar.update()
    print(bar)

    bar = ProgBar(len(collection_names), monitor=True,
                  title='Writing back to MongoDB from file')

    for col in collection_names:
        with open(col_file + col + '.json', 'r') as f:
            for tweet in f:
                try:
                    main_collection.insert(simplify(json.loads(tweet)))
                except KeyError:
                    continue
        bar.update()
    print(bar)

