from __future__ import print_function # In python 2.7

from flask import Flask
from flask import render_template
from flask import request

from os import listdir
from os.path import isfile, join

from server.Hangman import Hangman

import time
import sys

from server.query.SQLParsingError import SQLParsingError

app = Flask(__name__)
data_path = "./data/"


@app.route('/query/', methods=['GET','POST'])
def query():
    if request.method == 'POST':
        sql_str = request.form['query']
        print('[SQL]: \'' + sql_str + '\'', file=sys.stderr)

        # Send query to query parser
        try:
            t0 = time.time()
            ret = Hangman.execute(sql_str)
            t1 = time.time()
            ret_str = ""
            for i, line in enumerate(ret):
                ret_str += line
                if i != len(ret) - 1:
                    ret_str += "\n"

            print("\n\t|- %d records found in %f seconds" % (len(ret), (t1 - t0)))
            resp = app.make_response((ret_str, {'SQL-Error': False}))

        except SQLParsingError as e:
            resp = app.make_response((str(e), {'SQL-Error': True}))

        return resp

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


if __name__ == '__main__':
    app.run()