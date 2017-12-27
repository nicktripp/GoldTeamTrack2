import cmd
import requests
import time


class Client(cmd.Cmd):
    def __init__(self):
        cmd.Cmd.__init__(self)

    def nothing(self):
        pass

    def do_hello(self, extra):
        print("Hello World")
        print(self)
        print(extra)

    def emptyline(self):
        print("Arg: ")
        print(self)

    def do_list(self, _):
        r = requests.get("http://127.0.0.1:5000/list_tables/")
        print(r.text)

    def do_query(self, query):
        t1 = time.time()
        print(query)
        r = requests.post("http://127.0.0.1:5000/query/", data={"query": query})

        if r.headers['SQL-Error'] == 'False':
            records = r.text.split('\n')

            for record in records:
                print(record)

            print("\n\t|-" + str(time.time() - t1) + ' seconds elapsed')

        elif r.headers['SQL-Error'] == 'True':
            print(r.text)

        else:
            pass # TODO Raise error


if __name__ == "__main__":
    print("Hello, welcome to the SQL query program. Any input is terminated by a newline character. To view previous queries, simply use the up arrow key. Happy searching!")

    com = Client()
    com.cmdloop()
