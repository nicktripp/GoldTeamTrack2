# Python CSV Database Management System

Gold Team Track 2 is a team for CS 411 implementing a Database Management System in python, where all data is stored in CSV files and can be queried through a command line client. The project will consist of a client-side command line tool and a server-side custom CSV Database System.

### Contributors

* Ben is handling the command line tool interface, server setup and connections, and architecting system algorithms. 
* Nick is developing query parsing, query optimizing, and concurrency controls. 
* Jacob is implementing transactions, data indexing, and efficient table reading.

## Client Command Line Tool

We will offer a REPL that accepts queries and sends the queries to our server-side tools. The command line tool will make use of builtin python modules in order to support command history and server http requests.

More info on how to use the client to come (including help statements and examples)

## Server Database System

All data will be stored in CSV, we will create a python program that efficiently performs operations and queries on the data. The Database System is split into multiple modules:

* QueryParser
* QueryOptimizer
* ConcurrencyControl
* Transactions
* Tables

## Libraries
* python.http.client
* python.concurrent
* python.cmd
* https://github.com/andialbrecht/sqlparse
* http://flask.pocoo.org/
* http://pandas.pydata.org/

