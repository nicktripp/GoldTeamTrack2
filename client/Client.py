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

        print(query)
        t1 = time.time()
        r = requests.post("http://127.0.0.1:5000/query/", data={"query": query})
        t2 = time.time()

        if r.headers['SQL-Error'] == 'False':
            records = r.text.split('\n')

            print("\n%d Records Found:\n" % len(records))
            for record in records:
                print(record)

            print("\n\t|- %d records found in %f seconds" % (len(records), (t2 - t1)))

        elif r.headers['SQL-Error'] == 'True':
            print(r.text)

        else:
            pass # TODO Raise error


if __name__ == "__main__":
    print("Hello, welcome to the SQL query program. Any input is terminated by a newline character. To view previous queries, simply use the up arrow key. Happy searching!")

    com = Client()
    com.cmdloop()
