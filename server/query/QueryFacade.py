class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """
    def __init__(self):
        pass

    @staticmethod
    def query(select_columns, from_tables, where_conditions):
        """
        @param select_columns - a list of strings corresponding to the proper column name
        @param from_tables - a list of table filenames
        @param where_conditions - a list of conditions corresponding to the proper conditions
        """

        # Placeholder:
        return "SELECT " + str(select_columns) + " FROM " + str(from_tables) + " WHERE " + str(where_conditions) + \
                "\nSELECT " + str(select_columns.__class__) + " FROM " + str(from_tables.__class__) + " WHERE " + str(where_conditions.__class__)


        return "Success, that was a great query."
