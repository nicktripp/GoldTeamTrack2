class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """
    def __init__(self):
        pass

    def query(self, select, from, where):
        """
        Do what you need I can hook this up to the Table
        """
        return "Success, that was a great query."
