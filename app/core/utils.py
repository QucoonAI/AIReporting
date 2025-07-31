import logging
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    level=logging.ERROR
)
logger = logging.getLogger(__name__)


def read_from_sql_db(query: str, connection_string: str) -> pd.DataFrame:
    """
    Connects to a SQL database (e.g., MySQL, PostgreSQL) and executes a SELECT query.
    Returns the results as a pandas DataFrame.
    Raises a ValueError if the query is not a SELECT statement.
    """
    if not query.strip().upper().startswith('SELECT'):
        raise ValueError("This function is for read-only (SELECT) operations.")
    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            logger.info("Executing SELECT query.")
            # Use pd.read_sql for efficient reading into a DataFrame
            df = pd.read_sql(text(query), connection)
            return df
    except SQLAlchemyError as e:
        logger.error(f"SQL Database read error: {e}")
        # Re-raise the exception for the calling code to handle
        raise

def read_from_mongo_db(db_name: str, collection_name: str, filter_query: dict, connection_string: str) -> pd.DataFrame:
    """
    Queries a MongoDB collection and returns the results as a pandas DataFrame.
    Args:
        db_name: The name of the database.
        collection_name: The name of the collection.
        filter_query: The MongoDB query filter (e.g., {'status': 'active'}).
                      Use an empty dict {} to find all documents.
        connection_string: The MongoDB connection string.
    """
    try:
        # Using a 'with' statement ensures the connection is managed properly
        with MongoClient(connection_string) as client:
            db = client[db_name]
            collection = db[collection_name]
            logger.info(f"Querying MongoDB collection '{collection_name}' with filter: {filter_query}")
            # .find() performs the read operation
            cursor = collection.find(filter_query)
            # Convert the results to a DataFrame
            return pd.DataFrame(list(cursor))
    except PyMongoError as e:
        logger.error(f"MongoDB read error: {e}")
        # Re-raise the exception
        raise