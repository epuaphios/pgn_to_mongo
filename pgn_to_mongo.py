#!/usr/bin/env python
# -*- coding: utf-8 -*-
import chess.pgn
import re
import pymongo
from threading import Thread
from queue import *
from time import time

import os.path
import pathlib
import logging
from datetime import datetime
import sys

log = logging.getLogger().error
myclient = pymongo.MongoClient("mongodb://root:rootpassword@localhost:27017/")
mydb = myclient["chess"]
mycol = mydb["developers"]
queW = Queue()


dir_ = sys.argv[1]
if not os.path.exists(dir_):
    raise Exception(dir_ + ' not found')

is_join = False
if len(sys.argv) == 4:
    if sys.argv[3] == 'join':
        is_join = True

inp_dir = pathlib.Path(sys.argv[1])


def get_file_list(local_path):
    tree = os.walk(str(local_path))
    file_list = []
    out = []
    test = r'.+pgn$'
    for i in tree:
        file_list = i[2]

    for name in file_list:
        if (len(re.findall(test, name))):
            out.append(str(local_path / name))
    return out


def get_data(pgn_file):
    node = chess.pgn.read_game(pgn_file)

    while node is not None:
        data = node.headers
        data["moves"] = []

        while node.variations:
            next_node = node.variation(0)
            data["moves"].append(
                re.sub("\{.*?\}", "", node.board().san(next_node.move)))
            node = next_node

        node = chess.pgn.read_game(pgn_file)
        queW.put(data)
        print("que size add: " + str(queW.qsize()))
        # mongo_write(queW)


def mongo_write(queW):
    out_dict = {}
    data = queW.get()

    if len(data["moves"]) >= 20:
        for key in data.keys():
            if key == 'Result':
                out_dict['Result'] = result(data.get('Result'))
            if key == 'moves':
                out_dict['moves'] = data.get('moves')

        mycol.insert_one(out_dict)
        queW.task_done()
        print("que size delete: " + str(queW.qsize()))


def result(result):
    if result == '1-0':
        return "w"
    elif result == '0-1':
        return "b"
    else:
        return "d"


# def convert_file(file_path):
#     log('convert file ' + file_path.name)
#
#     pgn_file = open(str(file_path), encoding='ISO-8859-1')
#     get_data(pgn_file)


file_list = get_file_list(inp_dir)

start_time = datetime.now()
for file in file_list:
    pgn_file = open(str(file), encoding='ISO-8859-1')
    # log('convert file ' + file.name)

for i in range(5):
    t = Thread(target=get_data, args=(pgn_file,))
    t.daemon = True
    t.start()

for i in range(5):
    t = Thread(target=mongo_write, args=(queW,))
    t.daemon = True
    t.start()

queW.join()
print(datetime.now() - start_time)

end_time = datetime.now()
log('time ' + str(end_time - start_time))
