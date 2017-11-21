# Python CSV Database Management System

Gold Team Track 2 is a team for CS 411 implementing a Database Management System in python, where all data is stored in CSV files and can be queried through a command line client. The project will consist of a client-side command line tool and a server-side custom CSV Database System.

### Contributors

* Ben is handling the command line client and the query server, and developing alternative indexing techinques.
* Nick is developing query parsing and query optimizing.
* Jacob is developing the physical execution plan and the B+ Tree indexing.
* All will work together to support concurrently executing the stages of a query.

## Quick start

You need both the client and the server running.

In one window, clone this repo
* `cd GoldTeamTrack2`
* `pip install --upgrade sqlparse`
* `pip install Flask` 
* `python client/Client.py`

In another terminal window
* `cd GoldTeamTrack2`
* `export FLASK_APP='server/Server.py'`
* `flask run`

To execute a command, use the `query` keyword, double quotes for strings, and prepend column names with the table name ie `query SELECT movies.movie_title FROM movies WHERE movies.movie_title = "Spider-Man 3"`


## Client Command Line Tool

We will offer a REPL that accepts queries and sends the queries to our server-side tools. The command line tool will make use of builtin python modules in order to support command history and server http requests.

More info on how to use the client to come (including help statements and examples)

### (Current) Query Structure Ground Rules:
* Any quotes should be double quotes (""), not single quotes ('')
* Parenthesis are not currently handled
* All filepaths should be in quotations: i.e. "./movies.csv"

## Server Database System

All data is stored in a CSV in a data directory at the root. Before querying, create a `FileIndexer` instance in order to create and save a `BTreeIndex` for each column of `FileIndexer`. A `BTreeIndex` creates a B+ Tree over a column and supports comparison operations using its B+ Tree member. With the inidices created, you can run the client and server programs with commands from the Quickstart.

As development continues, we will work on the following modules to efficiently perform queries over the CSV data.

* QueryParser
* QueryOptimizer
* QueryFacade
* FileIndexer
* BTreeIndex
* BitmapIndex
* Server
* Client

## Libraries
* python.http.client
* python.concurrent
* python.cmd
* https://github.com/andialbrecht/sqlparse
* http://flask.pocoo.org/

