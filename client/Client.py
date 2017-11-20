import http
import cmd
import requests

class Client(cmd.Cmd):
    def __init__(self):
        cmd.Cmd.__init__(self)

    def nothing(self):
        pass

    def do_hello(self, extra):
        print("Hello World")
        print(self)
        print(extra)
        return

    def emptyline(self):
        print("Arg: ")
        print(self)
        return

    def do_query(self, query):
        print(query)
        r = requests.post("http://127.0.0.1:5000/query/", data={"query":query})

        print(r.text)

        return




history = []

print("Hello, welcome to the SQL query program. Any input is terminated by a newline character. To view previous queries, simply use the up arrow key. Happy searching!")

com = Client()
com.cmdloop()
