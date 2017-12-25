from __future__ import print_function # In python 2.7

from flask import Flask
from flask import render_template
from flask import request

from os import listdir
from os.path import isfile, join

from server.Hangman import Hangman

import sys

app = Flask(__name__)
data_path = "./data/"


@app.route('/query/', methods=['GET','POST'])
def query():
    if request.method == 'POST':
        sql_str = request.form['query']
        print('[SQL]: \'' + sql_str + '\'', file=sys.stderr)

        # Send query to query parser
        ret_val = Hangman.execute(sql_str)
        print(ret_val, file=sys.stderr)

        return ret_val

    else:
        return render_template('querypage.html')


@app.route('/list_tables/', methods=['GET'])
def list_tables():
    csv_files = [f for f in listdir(data_path) if isfile(join(data_path, f)) and f.endswith(".csv")]
    ret_val = ""
    for i,file in enumerate(csv_files):
        ret_val += file
        if i != len(csv_files)-1:
            ret_val += "\n"
    return ret_val
