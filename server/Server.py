from __future__ import print_function # In python 2.7

from flask import Flask
from flask import render_template
from flask import request
from flask import make_response

from server.query.Parser import Parser
from server.query.SQLParsingError import SQLParsingError

import sys

app = Flask(__name__)

@app.route('/')
def hello_world():
  return "Hello, World!"

@app.route('/hello/<name>/')
def hello(name=None):
    return render_template('hello.html', name=name)

@app.route('/query/', methods=['GET','POST'])
def query():
    if(request.method == 'POST'):
        sql_str = request.form['query']
        print('[SQL]: \'' + sql_str + '\'', file=sys.stderr)

        # Send query to query parser
        p = Parser(sql_str)
        try:
            ret = p.parse_select_from_where()
        except SQLParsingError as err:
            ret = "[ERROR]: SQL Parsing Error: " + str(err)
            print(ret, file=sys.stderr)

        return ret

    else:
        return render_template('querypage.html')