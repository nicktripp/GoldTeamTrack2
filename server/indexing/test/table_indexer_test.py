from server.indexing.TableIndexer import TableIndexer
from server.query.Table import Table

if __name__ == "__main__":
    Table.relative_path = '../../../data/%s.csv'
    TableIndexer.relative_path = '../../../data/'
    ti = TableIndexer(Table('review50k'))