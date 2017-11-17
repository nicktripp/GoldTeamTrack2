from server.indexing.BTree import BTree

if __name__ == "__main__":
    print("Testing B+ Tree Implementation")

    btree = BTree(4, {1: 'a', 2: 'b', 3: 'c', 4: 'd', 5: 'e'})
    print("Searching for %d in %s found %s" % (2, btree, btree.lookup(2)))

    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    for i in range(6, len(alphabet)): # ik
        btree.insert(i, alphabet[i])

    print("//////////////////////")

    # Screws up the other instance
    init = {}
    for i in range(11):
        init[i] = alphabet[i]
    print("Seeding with %s" % init)
    btree = BTree(10, init)
    print("Searching for %d in %s found %s" % (2, btree, btree.lookup(2)))

    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    for i in range(12, len(alphabet)): # ik
        btree.insert(i, alphabet[i])

    print("Searching for %d in %s found %s" % (17, btree, btree.lookup(17)))
