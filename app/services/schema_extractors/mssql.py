import pyodbc
import json


def extract_mssql_schema(connection_string: str, schema_name: str = 'dbo') -> str:
    """
    Extracts Microsoft SQL Server database schema and returns it as a JSON string.
    
    Args:
        connection_string (str): SQL Server connection string 
                                (e.g., "DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=db;UID=user;PWD=password")
        schema_name (str): Schema name to extract (default: 'dbo')
    
    Returns:
        str: JSON string containing the database schema information
        
    Raises:
        pyodbc.Error: If database connection or query fails
        json.JSONEncodeError: If JSON serialization fails
    """
    
    schema_info = {
        "schema_name": schema_name,
        "tables": {},
        "views": {},
        "functions": {},
        "sequences": {},
        "indexes": {},
        "constraints": {}
    }
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
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
            COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        cursor.execute(tables_query, schema_name)
        table_results = cursor.fetchall()
        
        # Process table results
        for row in table_results:
            table_name, col_name, data_type, char_max_len, num_precision, num_scale, is_nullable, col_default, ordinal_pos, is_identity = row
            
            if table_name not in schema_info["tables"]:
                schema_info["tables"][table_name] = {
                    "columns": {},
                    "primary_keys": [],
                    "foreign_keys": []
                }
            
            if col_name:  # Some tables might not have columns in the result
                schema_info["tables"][table_name]["columns"][col_name] = {
                    "data_type": data_type,
                    "character_maximum_length": char_max_len,
                    "numeric_precision": num_precision,
                    "numeric_scale": num_scale,
                    "is_nullable": is_nullable == 'YES',
                    "column_default": col_default,
                    "ordinal_position": ordinal_pos,
                    "is_identity": bool(is_identity)
                }
        
        # Extract primary keys
        pk_query = """
        SELECT 
            tc.TABLE_NAME,
            kcu.COLUMN_NAME,
            tc.CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.TABLE_SCHEMA = ?
        ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION
        """
        
        cursor.execute(pk_query, schema_name)
        pk_results = cursor.fetchall()
        
        for table_name, col_name, constraint_name in pk_results:
            if table_name in schema_info["tables"]:
                schema_info["tables"][table_name]["primary_keys"].append(col_name)
        
        # Extract foreign keys
        fk_query = """
        SELECT 
            fk.name AS constraint_name,
            tp.name AS parent_table,
            cp.name AS parent_column,
            tr.name AS referenced_table,
            cr.name AS referenced_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        INNER JOIN sys.schemas s ON tp.schema_id = s.schema_id
        WHERE s.name = ?
        """
        
        cursor.execute(fk_query, schema_name)
        fk_results = cursor.fetchall()
        
        for constraint_name, parent_table, parent_column, referenced_table, referenced_column in fk_results:
            if parent_table in schema_info["tables"]:
                schema_info["tables"][parent_table]["foreign_keys"].append({
                    "column": parent_column,
                    "references_table": referenced_table,
                    "references_column": referenced_column,
                    "constraint_name": constraint_name
                })
        
        # Extract views
        views_query = """
        SELECT 
            v.TABLE_NAME,
            vm.definition
        FROM INFORMATION_SCHEMA.VIEWS v
        INNER JOIN sys.views sv ON v.TABLE_NAME = sv.name
        INNER JOIN sys.sql_modules vm ON sv.object_id = vm.object_id
        INNER JOIN sys.schemas s ON sv.schema_id = s.schema_id
        WHERE s.name = ?
        """
        
        cursor.execute(views_query, schema_name)
        view_results = cursor.fetchall()
        
        for view_name, view_definition in view_results:
            schema_info["views"][view_name] = {
                "definition": view_definition.strip() if view_definition else None
            }
        
        # Extract sequences (SQL Server 2012+)
        sequences_query = """
        SELECT 
            s.name AS sequence_name,
            s.start_value,
            s.minimum_value,
            s.maximum_value,
            s.increment,
            s.current_value,
            t.name AS data_type
        FROM sys.sequences s
        INNER JOIN sys.types t ON s.user_type_id = t.user_type_id
        INNER JOIN sys.schemas sch ON s.schema_id = sch.schema_id
        WHERE sch.name = ?
        """
        
        cursor.execute(sequences_query, schema_name)
        sequence_results = cursor.fetchall()
        
        for seq_name, start_val, min_val, max_val, increment, current_val, data_type in sequence_results:
            schema_info["sequences"][seq_name] = {
                "data_type": data_type,
                "start_value": str(start_val),
                "minimum_value": str(min_val),
                "maximum_value": str(max_val),
                "increment": str(increment),
                "current_value": str(current_val)
            }
        
        # Extract indexes
        indexes_query = """
        SELECT 
            i.name AS index_name,
            t.name AS table_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND i.name IS NOT NULL
        GROUP BY i.name, t.name, i.type_desc, i.is_unique, i.is_primary_key
        ORDER BY t.name, i.name
        """
        
        cursor.execute(indexes_query, schema_name)
        index_results = cursor.fetchall()
        
        for index_name, table_name, index_type, is_unique, is_primary_key, columns in index_results:
            schema_info["indexes"][index_name] = {
                "table": table_name,
                "type": index_type,
                "is_unique": bool(is_unique),
                "is_primary_key": bool(is_primary_key),
                "columns": columns
            }
        
        # Extract check constraints
        check_constraints_query = """
        SELECT 
            cc.CONSTRAINT_NAME,
            cc.TABLE_NAME,
            cc.CHECK_CLAUSE
        FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
            ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE tc.TABLE_SCHEMA = ?
        """
        
        cursor.execute(check_constraints_query, schema_name)
        constraint_results = cursor.fetchall()
        
        for constraint_name, table_name, check_clause in constraint_results:
            schema_info["constraints"][constraint_name] = {
                "table": table_name,
                "type": "CHECK",
                "definition": check_clause
            }
        
        # Extract unique constraints
        unique_constraints_query = """
        SELECT 
            tc.CONSTRAINT_NAME,
            tc.TABLE_NAME,
            STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) AS columns
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' 
            AND tc.TABLE_SCHEMA = ?
        GROUP BY tc.CONSTRAINT_NAME, tc.TABLE_NAME
        """
        
        cursor.execute(unique_constraints_query, schema_name)
        unique_results = cursor.fetchall()
        
        for constraint_name, table_name, columns in unique_results:
            schema_info["constraints"][constraint_name] = {
                "table": table_name,
                "type": "UNIQUE",
                "columns": columns
            }
        
        # Extract stored procedures and functions
        functions_query = """
        SELECT 
            p.name AS routine_name,
            p.type_desc AS routine_type,
            m.definition
        FROM sys.procedures p
        INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
        INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE s.name = ?
        UNION ALL
        SELECT 
            f.name AS routine_name,
            f.type_desc AS routine_type,
            m.definition
        FROM sys.objects f
        INNER JOIN sys.sql_modules m ON f.object_id = m.object_id
        INNER JOIN sys.schemas s ON f.schema_id = s.schema_id
        WHERE s.name = ? AND f.type IN ('FN', 'IF', 'TF')
        """
        
        cursor.execute(functions_query, schema_name, schema_name)
        function_results = cursor.fetchall()
        
        for routine_name, routine_type, definition in function_results:
            schema_info["functions"][routine_name] = {
                "type": routine_type,
                "definition": definition.strip() if definition else None
            }
        
        cursor.close()
        conn.close()
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except pyodbc.Error as e:
        raise pyodbc.Error(f"Database error: {e}")
    except json.JSONEncodeError as e:
        raise json.JSONEncodeError(f"JSON serialization error: {e}")

# Example usage
if __name__ == "__main__":
    # Example connection strings for different authentication methods
    
    # SQL Server Authentication
    conn_str_sql_auth = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=server_name;"
        "DATABASE=database_name;"
        "UID=username;"
        "PWD=password;"
    )
    
    # Windows Authentication
    conn_str_windows_auth = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=server_name;"
        "DATABASE=database_name;"
        "Trusted_Connection=yes;"
    )
    
    # Azure SQL Database
    conn_str_azure = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=server_name.database.windows.net;"
        "DATABASE=database_name;"
        "UID=username@server_name;"
        "PWD=password;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    try:
        # Use one of the connection strings above
        schema_json = extract_mssql_schema(conn_str_sql_auth, 'dbo')
        print(schema_json)
        
        # Optionally save to file
        with open('mssql_database_schema.json', 'w') as f:
            f.write(schema_json)
            
    except Exception as e:
        print(f"Error extracting schema: {e}")