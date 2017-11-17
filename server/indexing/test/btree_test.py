from server.indexing.BTree import BTree

if __name__ == "__main__":
    print("Testing B+ Tree Implementation")

    btree = BTree(4, {1: 'a', 2: 'b', 3: 'c', 4: 'd', 5: 'e'})

    print("FAILURE")