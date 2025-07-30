import logging
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Set up logging to see informational messages and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

# # --- Example Usage ---
# if __name__ == '__main__':
#     # Add your actual connection strings here
#     pg_conn_str = "postgresql://user:password@host:port/database"
#     mongo_conn_str = "mongodb://user:password@host:port/"
#     try:
#         # SQL Read Example
#         sql_query = "SELECT product_name, price FROM products WHERE category = 'Electronics'"
#         # df_products = read_from_sql_db(sql_query, pg_conn_str)
#         # print("SQL Products:\n", df_products)
        
#         # MongoDB Read Example
#         mongo_filter = {"status": "shipped", "total_amount": {"$gt": 100}}
#         df_orders = read_from_mongo_db("ecommerce_db", "orders", mongo_filter, mongo_conn_str)
#         print("Mongo Orders:\n", df_orders)

#     except (ValueError, SQLAlchemyError, PyMongoError) as e:
#         # Catches bad query types (ValueError) or connection/database issues
#         print(f"An error occurred during a database read operation: {e}")