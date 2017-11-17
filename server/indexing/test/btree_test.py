from server.indexing.BTree import BTree
import pickle

if __name__ == "__main__":
    print("Testing B+ Tree Implementation")

    btree = BTree(4, {1: 'a', 2: 'b', 3: 'c', 4: 'd', 5: 'e'})
    print("Searching for %d in %s found %s" % (2, btree, btree.lookup(2)))
    print("Searching for %d in %s found %s" % (3, btree, btree.lookup(3)))

    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    for i in range(6, len(alphabet)):  # ik
        btree.insert(i, alphabet[i])

    print("//////////////////////")

    # Screws up the other instance by using a different blocksize
    init = {}
    for i in range(11):
        init[i] = alphabet[i]
    print("Seeding with %s" % init)
    btree = BTree(10, init)
    print("Searching for %d, found %s" % (2, btree.lookup(2)))

    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    for i in range(12, len(alphabet)):  # ik
        btree.insert(i, alphabet[i])

    print("Searching for %d, found %s" % (17, btree.lookup(17)))

    # Test saving to file works
    with open('./tmp', 'wb') as f:
        pickle.dump(btree, f, pickle.HIGHEST_PROTOCOL)

    revived = None
    with open('./tmp', 'rb') as f:
        revived = pickle.load(f)
    print("After dumping and loading //////////////")
    print("Searching for %d, found %s" % (17, revived.lookup(17)))

    print("Searching for %d, found %s" % (26, revived.lookup(26)))

    print("DONE")