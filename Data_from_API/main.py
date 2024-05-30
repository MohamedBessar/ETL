from dev.asker import Asker
from dev.database import PrepareDatabase
from dev.parser import Parser
from dev.inserter import Inserter
import pymysql

def main():
    # Establish the connection the target database
    database = PrepareDatabase()

    # Ingesting data from API source
    result = Asker()

    # Filter data and neglect data we don't need
    parser = Parser(response=result.json_response)

    # Insert the data into database
    Inserter(df=parser.parsed_response, engine=database.engine)

if __name__ == "__main__":
    main()