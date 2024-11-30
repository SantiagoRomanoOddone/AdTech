import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv


def db_query(DB_CONFIG, query: str) -> pd.DataFrame:
    """
    Function to query the database and return the results as a pandas DataFrame. 
    """
    # Connect to the database
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    # Query the database
    cursor.execute(f"{query}")
    result = cursor.fetchall()

    # Get column names
    colnames = [desc[0] for desc in cursor.description]

    # Close the connection
    cursor.close()
    connection.close()

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=colnames)

    return df



if __name__ == '__main__':

    load_dotenv()
    DB_CONFIG = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
    }

    # Example query
    query = "select distinct date from recommendations"

    # Query the database
    result = db_query(DB_CONFIG, query)

    print(result)


