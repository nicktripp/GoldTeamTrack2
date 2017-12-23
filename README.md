# Python CSV SQL Query Tool

For Project Track 2 in CS 411, Gold Team is implementing a system that accepts SELECT-FROM-WHERE SQL queries and performs the query on CSV files. The CSV files may be up to 2 million rows and 300 attributes. The tool will create indexes over the columns of the CSV tables of size up to 30% of the CSV files. Both single table queries and multi-table join queries are supported. There is a client facing command line client that provides a REPL with query history. Additionally, there is a flask server that accepts and processes the queries from the client.

### Contributors

* Ben Stoehr
* Nick Tripp
* Jacob Trueb

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

The csv files should be placed in the `./data` directory. In the following query, `movies` refers to `./data/movies.csv`. To execute a command, use the `query` keyword, double quotes for strings, and prepend column names with the table name ie `query SELECT movies.movie_title FROM movies WHERE movies.movie_title = "Spider-Man 3"`

## Client Command Line Tool

We will offer a REPL that accepts queries and sends the queries to our server-side tools. The command line tool will make use of builtin python modules in order to support command history and server http requests.

More info on how to use the client to come (including help statements and examples)

### (Current) Query Structure Ground Rules:
* Any quotes should be double quotes (""), not single quotes ('')
* All filepaths should be just the name of the file: './data/review1m.csv' can be referred to with `SELECT * FROM review1m`.
* CSV filenames may not contain `-`, so rename the files if they do.
* If conditionally joining tables, the conditions must be in parentheses: i.e. ON (<conditions>)

## Server Database System

Server-side, query execution is handled by `Hangman`. The `Hangman.execute` method accepts the SQL query string. It calls the necessary methods to execute the query processing pipeline, wrapping each call in a timing method that prints to the server's stdout. The result of the query is returned in the response from flask to the client.

The query execution pipeline includes a `QueryParser` that parses the query and validates the presence of each table and column referenced in the query. After parsing the query with `QueryParser.parse_select_from_where`, we pass the tables, columns, where_conditions, and join_conditions to the `QueryOptimizer`, where we determine the order that we will join tables and reduce the where_conditions algebraically. The output from the `QueryOptimizer` is passed into the `QueryFacade` to join the tables (using pandas) and select rows according to the conditions. Conditions are checked according to the index for a column. We have two different types of indexes `BitmapIndex` and `BTreeIndex`. Originally, we only had the `BTreeIndex`, but `BitmapIndex` was added in order to support faster partial matching. Indexes are loaded and managed by the `TableIndexer` object, which automatically creates, loads, and saves indexes as needed by the `QueryFacade`.

## Libraries
* python.http.client
* python.concurrent
* python.cmd
* https://github.com/andialbrecht/sqlparse
* https://pandas.pydata.org/
* http://flask.pocoo.org/

