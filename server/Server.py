from __future__ import print_function # In python 2.7

from flask import Flask
from flask import render_template
from flask import request
from flask import make_response

from server.query import Parser

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
        query_formatted = Parser.parse(sql_str)
        return make_response(query_formatted, 200)

    else:
        return render_template('querypage.html')