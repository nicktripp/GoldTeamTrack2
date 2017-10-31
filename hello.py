from flask import Flask
from flask import render_template
from flask import request
from flask import make_response

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
        print(request.data)
    resp = make_response(render_template('querypage.html'),200)
    resp.text = "I got it dawg!"
    return resp
