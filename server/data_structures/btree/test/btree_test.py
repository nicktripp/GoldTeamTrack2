from server.data_structures.btree.btree import BTree

if __name__ == "__main__":
    print("Testing BTree implementation")

    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    initial_values = {k: v for k, v in zip(alphabet, range(3))}
    btree = BTree(4, initial_values)

    # Assert that initialization works
    assert (str(btree) == \
            """\
{Internal ['b'] :: [
\t{External ['a'] :: [0]}
\t{External ['b', 'c'] :: [1, 2]}
]}""")
    for k in initial_values:
        assert (btree[k] == initial_values[k])

    # Assert that insert without split works
    for k, v in zip(alphabet[3:], range(3, 5)):
        btree[k] = v
        assert (btree[k] == v)
    assert (str(btree) == \
            """\
{Internal ['b'] :: [
\t{External ['a'] :: [0]}
\t{External ['b', 'c', 'd', 'e'] :: [1, 2, 3, 4]}
]}""")

    # Assert that insert with leaf split works
    k, v = alphabet[5], 5
    btree[k] = v
    assert (btree[k] == v)
    assert (str(btree) == \
            """\
{Internal ['b', 'd'] :: [
\t{External ['a'] :: [0]}
\t{External ['b', 'c'] :: [1, 2]}
\t{External ['d', 'e', 'f'] :: [3, 4, 5]}
]}""")

    # Fill to prepare for root split
    for k, v in zip(alphabet[6:], range(6, 11)):
        btree[k] = v
        assert (btree[k] == v)
    assert (str(btree) == \
            """\
{Internal ['b', 'd', 'f', 'h'] :: [
\t{External ['a'] :: [0]}
\t{External ['b', 'c'] :: [1, 2]}
\t{External ['d', 'e'] :: [3, 4]}
\t{External ['f', 'g'] :: [5, 6]}
\t{External ['h', 'i', 'j', 'k'] :: [7, 8, 9, 10]}
]}""")

    # Assert root split works
    k, v = alphabet[11], 11
    btree[k] = v
    assert (btree[k] == v)
    assert (str(btree) == \
            """\
{Internal ['f'] :: [
	Internal ['b', 'd'] :: [
		{External ['a'] :: [0]}	
		{External ['b', 'c'] :: [1, 2]}	
		{External ['d', 'e'] :: [3, 4]}
	]}
	Internal ['h', 'j'] :: [
		{External ['f', 'g'] :: [5, 6]}	
		{External ['h', 'i'] :: [7, 8]}	
		{External ['j', 'k', 'l'] :: [9, 10, 11]}
	]}
]}""")

    # Assert split root again works
    for k, v in zip(alphabet[12:], range(12, 26)):
        btree[k] = v
        assert (btree[k] == v)

    zlphabet = ['z' + c for c in alphabet]
    for k, v in zip(zlphabet, range(26)):
        btree[k] = v
        assert (btree[k] == v)

    zzlphabet = ['z' + c for c in zlphabet]
    for k, v in zip(zzlphabet, range(2)):
        btree[k] = v
        assert (btree[k] == v)
    assert (str(btree) == \
            """\
{Internal ['r', 'zj'] :: [
	Internal ['f', 'l'] :: [
		Internal ['b', 'd'] :: [
			{External ['a'] :: [0]}		
			{External ['b', 'c'] :: [1, 2]}		
			{External ['d', 'e'] :: [3, 4]}
		]}	
		Internal ['h', 'j'] :: [
			{External ['f', 'g'] :: [5, 6]}		
			{External ['h', 'i'] :: [7, 8]}		
			{External ['j', 'k'] :: [9, 10]}
		]}	
		Internal ['n', 'p'] :: [
			{External ['l', 'm'] :: [11, 12]}		
			{External ['n', 'o'] :: [13, 14]}		
			{External ['p', 'q'] :: [15, 16]}
		]}
	]}
	Internal ['x', 'zd'] :: [
		Internal ['t', 'v'] :: [
			{External ['r', 's'] :: [17, 18]}		
			{External ['t', 'u'] :: [19, 20]}		
			{External ['v', 'w'] :: [21, 22]}
		]}	
		Internal ['z', 'zb'] :: [
			{External ['x', 'y'] :: [23, 24]}		
			{External ['z', 'za'] :: [25, 0]}		
			{External ['zb', 'zc'] :: [1, 2]}
		]}	
		Internal ['zf', 'zh'] :: [
			{External ['zd', 'ze'] :: [3, 4]}		
			{External ['zf', 'zg'] :: [5, 6]}		
			{External ['zh', 'zi'] :: [7, 8]}
		]}
	]}
	Internal ['zp', 'zv'] :: [
		Internal ['zl', 'zn'] :: [
			{External ['zj', 'zk'] :: [9, 10]}		
			{External ['zl', 'zm'] :: [11, 12]}		
			{External ['zn', 'zo'] :: [13, 14]}
		]}	
		Internal ['zr', 'zt'] :: [
			{External ['zp', 'zq'] :: [15, 16]}		
			{External ['zr', 'zs'] :: [17, 18]}		
			{External ['zt', 'zu'] :: [19, 20]}
		]}	
		Internal ['zx', 'zz'] :: [
			{External ['zv', 'zw'] :: [21, 22]}		
			{External ['zx', 'zy'] :: [23, 24]}		
			{External ['zz', 'zza', 'zzb'] :: [25, 0, 1]}
		]}
	]}
]}""")

    print("SUCCESS")
