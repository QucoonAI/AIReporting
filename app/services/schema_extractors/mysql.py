import mysql.connector
import json


def extract_mysql_schema(connection_config: dict, database_name: str) -> str:
    """
    Extracts MySQL database schema and returns it as a JSON string.
    
    Args:
        connection_config (dict): MySQL connection configuration
                                 Example: {
                                     'host': 'localhost',
                                     'user': 'username',
                                     'password': 'password',
                                     'database': 'database_name',
                                     'port': 3306
                                 }
        database_name (str): Database name to extract schema from
    
    Returns:
        str: JSON string containing the database schema information
        
    Raises:
        mysql.connector.Error: If database connection or query fails
        json.JSONEncodeError: If JSON serialization fails
    """
    
    schema_info = {
        "database_name": database_name,
        "tables": {},
        "views": {},
        "functions": {},
        "procedures": {},
        "triggers": {},
        "indexes": {},
        "constraints": {}
    }
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**connection_config)
        cursor = conn.cursor(dictionary=True)
        
        # Extract tables and their columns
        tables_query = """
        SELECT 
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION,
            c.EXTRA,
            c.COLUMN_TYPE,
            c.COLUMN_KEY,
            c.COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS c 
            ON t.TABLE_NAME = c.TABLE_NAME 
            AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_SCHEMA = %s AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        cursor.execute(tables_query, (database_name,))
        table_results = cursor.fetchall()
        
        # Process table results
        for row in table_results:
            table_name = row['TABLE_NAME']
            col_name = row['COLUMN_NAME']
            
            if table_name not in schema_info["tables"]:
                schema_info["tables"][table_name] = {
                    "columns": {},
                    "primary_keys": [],
                    "foreign_keys": [],
                    "indexes": [],
                    "engine": None,
                    "charset": None,
                    "collation": None
                }
            
            if col_name:  # Some tables might not have columns in the result
                schema_info["tables"][table_name]["columns"][col_name] = {
                    "data_type": row['DATA_TYPE'],
                    "column_type": row['COLUMN_TYPE'],
                    "character_maximum_length": row['CHARACTER_MAXIMUM_LENGTH'],
                    "numeric_precision": row['NUMERIC_PRECISION'],
                    "numeric_scale": row['NUMERIC_SCALE'],
                    "is_nullable": row['IS_NULLABLE'] == 'YES',
                    "column_default": row['COLUMN_DEFAULT'],
                    "ordinal_position": row['ORDINAL_POSITION'],
                    "extra": row['EXTRA'],
                    "column_key": row['COLUMN_KEY'],
                    "column_comment": row['COLUMN_COMMENT']
                }
        
        # Get table details (engine, charset, collation)
        table_details_query = """
        SELECT 
            TABLE_NAME,
            ENGINE,
            TABLE_COLLATION
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """
        
        cursor.execute(table_details_query, (database_name,))
        table_details = cursor.fetchall()
        
        for row in table_details:
            table_name = row['TABLE_NAME']
            if table_name in schema_info["tables"]:
                schema_info["tables"][table_name]["engine"] = row['ENGINE']
                schema_info["tables"][table_name]["collation"] = row['TABLE_COLLATION']
                # Extract charset from collation (e.g., utf8mb4_unicode_ci -> utf8mb4)
                if row['TABLE_COLLATION']:
                    schema_info["tables"][table_name]["charset"] = row['TABLE_COLLATION'].split('_')[0]
        
        # Extract primary keys
        pk_query = """
        SELECT 
            tc.TABLE_NAME,
            kcu.COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.TABLE_SCHEMA = %s
        ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION
        """
        
        cursor.execute(pk_query, (database_name,))
        pk_results = cursor.fetchall()
        
        for row in pk_results:
            table_name = row['TABLE_NAME']
            col_name = row['COLUMN_NAME']
            if table_name in schema_info["tables"]:
                schema_info["tables"][table_name]["primary_keys"].append(col_name)
        
        # Extract foreign keys
        fk_query = """
        SELECT 
            kcu.TABLE_NAME,
            kcu.COLUMN_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME,
            rc.UPDATE_RULE,
            rc.DELETE_RULE
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
            ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
        WHERE kcu.TABLE_SCHEMA = %s 
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """
        
        cursor.execute(fk_query, (database_name,))
        fk_results = cursor.fetchall()
        
        for row in fk_results:
            table_name = row['TABLE_NAME']
            if table_name in schema_info["tables"]:
                schema_info["tables"][table_name]["foreign_keys"].append({
                    "column": row['COLUMN_NAME'],
                    "references_table": row['REFERENCED_TABLE_NAME'],
                    "references_column": row['REFERENCED_COLUMN_NAME'],
                    "constraint_name": row['CONSTRAINT_NAME'],
                    "update_rule": row['UPDATE_RULE'],
                    "delete_rule": row['DELETE_RULE']
                })
        
        # Extract views
        views_query = """
        SELECT 
            TABLE_NAME,
            VIEW_DEFINITION,
            CHECK_OPTION,
            IS_UPDATABLE,
            SECURITY_TYPE
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = %s
        """
        
        cursor.execute(views_query, (database_name,))
        view_results = cursor.fetchall()
        
        for row in view_results:
            view_name = row['TABLE_NAME']
            schema_info["views"][view_name] = {
                "definition": row['VIEW_DEFINITION'],
                "check_option": row['CHECK_OPTION'],
                "is_updatable": row['IS_UPDATABLE'] == 'YES',
                "security_type": row['SECURITY_TYPE']
            }
        
        # Extract stored procedures
        procedures_query = """
        SELECT 
            ROUTINE_NAME,
            ROUTINE_TYPE,
            ROUTINE_DEFINITION,
            SECURITY_TYPE,
            SQL_DATA_ACCESS,
            ROUTINE_COMMENT
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'PROCEDURE'
        """
        
        cursor.execute(procedures_query, (database_name,))
        procedure_results = cursor.fetchall()
        
        for row in procedure_results:
            procedure_name = row['ROUTINE_NAME']
            schema_info["procedures"][procedure_name] = {
                "type": row['ROUTINE_TYPE'],
                "definition": row['ROUTINE_DEFINITION'],
                "security_type": row['SECURITY_TYPE'],
                "sql_data_access": row['SQL_DATA_ACCESS'],
                "comment": row['ROUTINE_COMMENT']
            }
        
        # Extract functions
        functions_query = """
        SELECT 
            ROUTINE_NAME,
            ROUTINE_TYPE,
            ROUTINE_DEFINITION,
            SECURITY_TYPE,
            SQL_DATA_ACCESS,
            ROUTINE_COMMENT,
            DTD_IDENTIFIER as RETURN_TYPE
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'FUNCTION'
        """
        
        cursor.execute(functions_query, (database_name,))
        function_results = cursor.fetchall()
        
        for row in function_results:
            function_name = row['ROUTINE_NAME']
            schema_info["functions"][function_name] = {
                "type": row['ROUTINE_TYPE'],
                "definition": row['ROUTINE_DEFINITION'],
                "return_type": row['RETURN_TYPE'],
                "security_type": row['SECURITY_TYPE'],
                "sql_data_access": row['SQL_DATA_ACCESS'],
                "comment": row['ROUTINE_COMMENT']
            }
        
        # Extract triggers
        triggers_query = """
        SELECT 
            TRIGGER_NAME,
            EVENT_MANIPULATION,
            EVENT_OBJECT_TABLE,
            ACTION_STATEMENT,
            ACTION_TIMING,
            ACTION_ORIENTATION
        FROM INFORMATION_SCHEMA.TRIGGERS
        WHERE TRIGGER_SCHEMA = %s
        """
        
        cursor.execute(triggers_query, (database_name,))
        trigger_results = cursor.fetchall()
        
        for row in trigger_results:
            trigger_name = row['TRIGGER_NAME']
            schema_info["triggers"][trigger_name] = {
                "event": row['EVENT_MANIPULATION'],
                "table": row['EVENT_OBJECT_TABLE'],
                "timing": row['ACTION_TIMING'],
                "orientation": row['ACTION_ORIENTATION'],
                "statement": row['ACTION_STATEMENT']
            }
        
        # Extract indexes (using SHOW INDEX for more detailed info)
        cursor_raw = conn.cursor()
        
        for table_name in schema_info["tables"].keys():
            cursor_raw.execute(f"SHOW INDEX FROM `{table_name}` FROM `{database_name}`")
            index_results = cursor_raw.fetchall()
            
            index_info = {}
            for row in index_results:
                index_name = row[2]  # Key_name
                if index_name not in index_info:
                    index_info[index_name] = {
                        "table": table_name,
                        "columns": [],
                        "is_unique": row[1] == 0,  # Non_unique (0 = unique, 1 = non-unique)
                        "type": row[10] if len(row) > 10 else None,  # Index_type
                        "comment": row[11] if len(row) > 11 else None  # Index_comment
                    }
                
                index_info[index_name]["columns"].append({
                    "column_name": row[4],  # Column_name
                    "sequence": row[3],     # Seq_in_index
                    "collation": row[7],    # Collation
                    "cardinality": row[6],  # Cardinality
                    "sub_part": row[8]      # Sub_part
                })
            
            # Sort columns by sequence
            for idx_name, idx_data in index_info.items():
                idx_data["columns"].sort(key=lambda x: x["sequence"])
                schema_info["indexes"][f"{table_name}.{idx_name}"] = idx_data
        
        # Extract check constraints (MySQL 8.0+)
        try:
            check_constraints_query = """
            SELECT 
                CONSTRAINT_NAME,
                TABLE_NAME,
                CHECK_CLAUSE
            FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = %s
            """
            
            cursor.execute(check_constraints_query, (database_name,))
            constraint_results = cursor.fetchall()
            
            for row in constraint_results:
                constraint_name = row['CONSTRAINT_NAME']
                schema_info["constraints"][constraint_name] = {
                    "table": row['TABLE_NAME'],
                    "type": "CHECK",
                    "definition": row['CHECK_CLAUSE']
                }
        except mysql.connector.ProgrammingError:
            # CHECK_CONSTRAINTS table doesn't exist in older MySQL versions
            pass
        
        # Extract unique constraints
        unique_constraints_query = """
        SELECT 
            tc.CONSTRAINT_NAME,
            tc.TABLE_NAME,
            GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' 
            AND tc.TABLE_SCHEMA = %s
        GROUP BY tc.CONSTRAINT_NAME, tc.TABLE_NAME
        """
        
        cursor.execute(unique_constraints_query, (database_name,))
        unique_results = cursor.fetchall()
        
        for row in unique_results:
            constraint_name = row['CONSTRAINT_NAME']
            schema_info["constraints"][constraint_name] = {
                "table": row['TABLE_NAME'],
                "type": "UNIQUE",
                "columns": row['COLUMNS']
            }
        
        cursor.close()
        cursor_raw.close()
        conn.close()
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except mysql.connector.Error as e:
        raise mysql.connector.Error(f"Database error: {e}")
    except json.JSONEncodeError as e:
        raise json.JSONEncodeError(f"JSON serialization error: {e}")

# Alternative function using connection string
def extract_mysql_schema_from_string(connection_string: str, database_name: str) -> str:
    """
    Alternative function that accepts a MySQL connection string.
    
    Args:
        connection_string (str): MySQL connection string in format:
                                "mysql://user:password@host:port/database"
                                or "mysql+pymysql://user:password@host:port/database"
        database_name (str): Database name to extract schema from
    
    Returns:
        str: JSON string containing the database schema information
    """
    import urllib.parse
    
    # Parse connection string
    parsed = urllib.parse.urlparse(connection_string)
    
    connection_config = {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or 3306,
        'user': parsed.username,
        'password': parsed.password,
        'database': database_name
    }
    
    return extract_mysql_schema(connection_config, database_name)

# Example usage
if __name__ == "__main__":
    # Example connection configuration
    config = {
        'host': 'localhost',
        'user': 'username',
        'password': 'password',
        'database': 'mydb',
        'port': 3306
    }
    
    # Alternative connection strings
    connection_string_examples = [
        "mysql://user:password@localhost:3306/mydb",
        "mysql://user:password@localhost/mydb",  # Default port
        "mysql://user@localhost/mydb",           # No password
    ]
    
    try:
        # Method 1: Using connection config dict
        schema_json = extract_mysql_schema(config, 'mydb')
        print(schema_json)
        
        # Method 2: Using connection string
        # schema_json = extract_mysql_schema_from_string(
        #     "mysql://user:password@localhost:3306/mydb", 
        #     'mydb'
        # )
        
        # Optionally save to file
        with open('mysql_database_schema.json', 'w') as f:
            f.write(schema_json)
            
    except Exception as e:
        print(f"Error extracting schema: {e}")