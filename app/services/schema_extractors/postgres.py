import psycopg
import json


def extract_postgres_schema(connection_string: str, schema_name: str = 'public') -> str:
    """
    Extracts PostgreSQL database schema and returns it as a JSON string.
    
    Args:
        connection_string (str): PostgreSQL connection string 
                                (e.g., "postgresql://user:password@localhost:5432/dbname")
        schema_name (str): Schema name to extract (default: 'public')
    
    Returns:
        str: JSON string containing the database schema information
        
    Raises:
        psycopg.Error: If database connection or query fails
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
        # Connect to PostgreSQL using psycopg 3
        with psycopg.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                
                # Extract tables and their columns
                tables_query = """
                SELECT 
                    t.table_name,
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    c.column_default,
                    c.ordinal_position,
                    c.udt_name,
                    CASE 
                        WHEN c.column_default LIKE 'nextval%' THEN true
                        ELSE false
                    END as is_serial
                FROM information_schema.tables t
                LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
                    AND t.table_schema = c.table_schema
                WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name, c.ordinal_position
                """
                
                cursor.execute(tables_query, (schema_name,))
                table_results = cursor.fetchall()
                
                # Process table results
                for row in table_results:
                    table_name, col_name, data_type, char_max_len, num_precision, num_scale, is_nullable, col_default, ordinal_pos, udt_name, is_serial = row
                    
                    if table_name not in schema_info["tables"]:
                        schema_info["tables"][table_name] = {
                            "columns": {},
                            "primary_keys": [],
                            "foreign_keys": []
                        }
                    
                    if col_name:  # Some tables might not have columns in the result
                        schema_info["tables"][table_name]["columns"][col_name] = {
                            "data_type": data_type,
                            "udt_name": udt_name,
                            "character_maximum_length": char_max_len,
                            "numeric_precision": num_precision,
                            "numeric_scale": num_scale,
                            "is_nullable": is_nullable == 'YES',
                            "column_default": col_default,
                            "ordinal_position": ordinal_pos,
                            "is_serial": is_serial
                        }
                
                # Extract primary keys
                pk_query = """
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    kcu.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' 
                    AND tc.table_schema = %s
                ORDER BY tc.table_name, kcu.ordinal_position
                """
                
                cursor.execute(pk_query, (schema_name,))
                pk_results = cursor.fetchall()
                
                for table_name, col_name, ordinal_pos in pk_results:
                    if table_name in schema_info["tables"]:
                        schema_info["tables"][table_name]["primary_keys"].append(col_name)
                
                # Extract foreign keys
                fk_query = """
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    tc.constraint_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name = rc.constraint_name
                    AND tc.table_schema = rc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_schema = %s
                """
                
                cursor.execute(fk_query, (schema_name,))
                fk_results = cursor.fetchall()
                
                for table_name, col_name, foreign_table, foreign_col, constraint_name, update_rule, delete_rule in fk_results:
                    if table_name in schema_info["tables"]:
                        schema_info["tables"][table_name]["foreign_keys"].append({
                            "column": col_name,
                            "references_table": foreign_table,
                            "references_column": foreign_col,
                            "constraint_name": constraint_name,
                            "update_rule": update_rule,
                            "delete_rule": delete_rule
                        })
                
                # Extract views
                views_query = """
                SELECT 
                    table_name,
                    view_definition,
                    check_option,
                    is_updatable,
                    is_insertable_into,
                    is_trigger_updatable,
                    is_trigger_deletable,
                    is_trigger_insertable_into
                FROM information_schema.views
                WHERE table_schema = %s
                """
                
                cursor.execute(views_query, (schema_name,))
                view_results = cursor.fetchall()
                
                for view_name, view_definition, check_option, is_updatable, is_insertable_into, is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into in view_results:
                    schema_info["views"][view_name] = {
                        "definition": view_definition,
                        "check_option": check_option,
                        "is_updatable": is_updatable == 'YES',
                        "is_insertable_into": is_insertable_into == 'YES',
                        "is_trigger_updatable": is_trigger_updatable == 'YES',
                        "is_trigger_deletable": is_trigger_deletable == 'YES',
                        "is_trigger_insertable_into": is_trigger_insertable_into == 'YES'
                    }
                
                # Extract sequences
                sequences_query = """
                SELECT 
                    sequence_name,
                    data_type,
                    start_value,
                    minimum_value,
                    maximum_value,
                    increment,
                    cycle_option
                FROM information_schema.sequences
                WHERE sequence_schema = %s
                """
                
                cursor.execute(sequences_query, (schema_name,))
                sequence_results = cursor.fetchall()
                
                for seq_name, data_type, start_val, min_val, max_val, increment, cycle_option in sequence_results:
                    schema_info["sequences"][seq_name] = {
                        "data_type": data_type,
                        "start_value": str(start_val),
                        "minimum_value": str(min_val),
                        "maximum_value": str(max_val),
                        "increment": str(increment),
                        "cycle_option": cycle_option == 'YES'
                    }
                
                # Extract indexes using PostgreSQL system tables
                indexes_query = """
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = %s
                """
                
                cursor.execute(indexes_query, (schema_name,))
                index_results = cursor.fetchall()
                
                for schema, table_name, index_name, index_def in index_results:
                    schema_info["indexes"][index_name] = {
                        "table": table_name,
                        "definition": index_def
                    }
                
                # Extract detailed index information
                detailed_indexes_query = """
                SELECT 
                    i.relname as index_name,
                    t.relname as table_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary,
                    ix.indisexclusion as is_exclusion,
                    ix.indimmediate as is_immediate,
                    ix.indisclustered as is_clustered,
                    ix.indisvalid as is_valid,
                    am.amname as access_method,
                    array_agg(a.attname ORDER BY a.attnum) as columns
                FROM pg_index ix
                JOIN pg_class i ON ix.indexrelid = i.oid
                JOIN pg_class t ON ix.indrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                JOIN pg_am am ON i.relam = am.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE n.nspname = %s
                GROUP BY i.relname, t.relname, ix.indisunique, ix.indisprimary, 
                         ix.indisexclusion, ix.indimmediate, ix.indisclustered, 
                         ix.indisvalid, am.amname
                """
                
                cursor.execute(detailed_indexes_query, (schema_name,))
                detailed_index_results = cursor.fetchall()
                
                for index_name, table_name, is_unique, is_primary, is_exclusion, is_immediate, is_clustered, is_valid, access_method, columns in detailed_index_results:
                    if index_name in schema_info["indexes"]:
                        schema_info["indexes"][index_name].update({
                            "is_unique": is_unique,
                            "is_primary": is_primary,
                            "is_exclusion": is_exclusion,
                            "is_immediate": is_immediate,
                            "is_clustered": is_clustered,
                            "is_valid": is_valid,
                            "access_method": access_method,
                            "columns": list(columns) if columns else []
                        })
                
                # Extract check constraints
                check_constraints_query = """
                SELECT 
                    tc.table_name,
                    tc.constraint_name,
                    cc.check_clause
                FROM information_schema.table_constraints tc
                JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name
                WHERE tc.table_schema = %s 
                    AND tc.constraint_type = 'CHECK'
                """
                
                cursor.execute(check_constraints_query, (schema_name,))
                constraint_results = cursor.fetchall()
                
                for table_name, constraint_name, check_clause in constraint_results:
                    schema_info["constraints"][constraint_name] = {
                        "table": table_name,
                        "type": "CHECK",
                        "definition": check_clause
                    }
                
                # Extract unique constraints
                unique_constraints_query = """
                SELECT 
                    tc.constraint_name,
                    tc.table_name,
                    string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'UNIQUE' 
                    AND tc.table_schema = %s
                GROUP BY tc.constraint_name, tc.table_name
                """
                
                cursor.execute(unique_constraints_query, (schema_name,))
                unique_results = cursor.fetchall()
                
                for constraint_name, table_name, columns in unique_results:
                    schema_info["constraints"][constraint_name] = {
                        "table": table_name,
                        "type": "UNIQUE",
                        "columns": columns
                    }
                
                # Extract functions and procedures
                functions_query = """
                SELECT 
                    r.routine_name,
                    r.routine_type,
                    r.data_type as return_type,
                    r.routine_definition,
                    r.external_language,
                    r.security_type,
                    r.sql_data_access,
                    r.is_deterministic,
                    p.prosrc as source_code
                FROM information_schema.routines r
                LEFT JOIN pg_proc p ON r.routine_name = p.proname
                LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE r.routine_schema = %s
                    AND (n.nspname = %s OR n.nspname IS NULL)
                """
                
                cursor.execute(functions_query, (schema_name, schema_name))
                function_results = cursor.fetchall()
                
                for routine_name, routine_type, return_type, routine_definition, external_language, security_type, sql_data_access, is_deterministic, source_code in function_results:
                    schema_info["functions"][routine_name] = {
                        "type": routine_type,
                        "return_type": return_type,
                        "definition": routine_definition or source_code,
                        "language": external_language,
                        "security_type": security_type,
                        "sql_data_access": sql_data_access,
                        "is_deterministic": is_deterministic == 'YES'
                    }
                
                # Extract triggers
                triggers_query = """
                SELECT 
                    t.trigger_name,
                    t.event_manipulation,
                    t.event_object_table,
                    t.action_statement,
                    t.action_timing,
                    t.action_orientation
                FROM information_schema.triggers t
                WHERE t.trigger_schema = %s
                """
                
                cursor.execute(triggers_query, (schema_name,))
                trigger_results = cursor.fetchall()
                
                for trigger_name, event_manipulation, event_object_table, action_statement, action_timing, action_orientation in trigger_results:
                    if trigger_name not in schema_info["functions"]:
                        schema_info["functions"][trigger_name] = {}
                    
                    # Store trigger info in functions section with special type
                    schema_info["functions"][trigger_name] = {
                        "type": "TRIGGER",
                        "event": event_manipulation,
                        "table": event_object_table,
                        "timing": action_timing,
                        "orientation": action_orientation,
                        "statement": action_statement
                    }
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except psycopg.Error as e:
        raise psycopg.Error(f"Database error: {e}")
    except json.JSONEncodeError as e:
        raise json.JSONEncodeError(f"JSON serialization error: {e}")

# Alternative async version for better performance
async def extract_postgres_schema_async(connection_string: str, schema_name: str = 'public') -> str:
    """
    Async version of the PostgreSQL schema extraction function.
    
    Args:
        connection_string (str): PostgreSQL connection string
        schema_name (str): Schema name to extract (default: 'public')
    
    Returns:
        str: JSON string containing the database schema information
    """
    import psycopg
    
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
        async with await psycopg.AsyncConnection.connect(connection_string) as conn:
            async with conn.cursor() as cursor:
                
                # Extract tables and their columns
                tables_query = """
                SELECT 
                    t.table_name,
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    c.column_default,
                    c.ordinal_position,
                    c.udt_name,
                    CASE 
                        WHEN c.column_default LIKE 'nextval%' THEN true
                        ELSE false
                    END as is_serial
                FROM information_schema.tables t
                LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
                    AND t.table_schema = c.table_schema
                WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name, c.ordinal_position
                """
                
                await cursor.execute(tables_query, (schema_name,))
                table_results = await cursor.fetchall()
                
                # Process table results
                for row in table_results:
                    table_name, col_name, data_type, char_max_len, num_precision, num_scale, is_nullable, col_default, ordinal_pos, udt_name, is_serial = row
                    
                    if table_name not in schema_info["tables"]:
                        schema_info["tables"][table_name] = {
                            "columns": {},
                            "primary_keys": [],
                            "foreign_keys": []
                        }
                    
                    if col_name:  # Some tables might not have columns in the result
                        schema_info["tables"][table_name]["columns"][col_name] = {
                            "data_type": data_type,
                            "udt_name": udt_name,
                            "character_maximum_length": char_max_len,
                            "numeric_precision": num_precision,
                            "numeric_scale": num_scale,
                            "is_nullable": is_nullable == 'YES',
                            "column_default": col_default,
                            "ordinal_position": ordinal_pos,
                            "is_serial": is_serial
                        }
                
                # Extract primary keys
                pk_query = """
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    kcu.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' 
                    AND tc.table_schema = %s
                ORDER BY tc.table_name, kcu.ordinal_position
                """
                
                await cursor.execute(pk_query, (schema_name,))
                pk_results = await cursor.fetchall()
                
                for table_name, col_name, ordinal_pos in pk_results:
                    if table_name in schema_info["tables"]:
                        schema_info["tables"][table_name]["primary_keys"].append(col_name)
                
                # Extract foreign keys
                fk_query = """
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    tc.constraint_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name = rc.constraint_name
                    AND tc.table_schema = rc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_schema = %s
                """
                
                await cursor.execute(fk_query, (schema_name,))
                fk_results = await cursor.fetchall()
                
                for table_name, col_name, foreign_table, foreign_col, constraint_name, update_rule, delete_rule in fk_results:
                    if table_name in schema_info["tables"]:
                        schema_info["tables"][table_name]["foreign_keys"].append({
                            "column": col_name,
                            "references_table": foreign_table,
                            "references_column": foreign_col,
                            "constraint_name": constraint_name,
                            "update_rule": update_rule,
                            "delete_rule": delete_rule
                        })
                
                # Extract views
                views_query = """
                SELECT 
                    table_name,
                    view_definition,
                    check_option,
                    is_updatable,
                    is_insertable_into,
                    is_trigger_updatable,
                    is_trigger_deletable,
                    is_trigger_insertable_into
                FROM information_schema.views
                WHERE table_schema = %s
                """
                
                await cursor.execute(views_query, (schema_name,))
                view_results = await cursor.fetchall()
                
                for view_name, view_definition, check_option, is_updatable, is_insertable_into, is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into in view_results:
                    schema_info["views"][view_name] = {
                        "definition": view_definition,
                        "check_option": check_option,
                        "is_updatable": is_updatable == 'YES',
                        "is_insertable_into": is_insertable_into == 'YES',
                        "is_trigger_updatable": is_trigger_updatable == 'YES',
                        "is_trigger_deletable": is_trigger_deletable == 'YES',
                        "is_trigger_insertable_into": is_trigger_insertable_into == 'YES'
                    }
                
                # Extract sequences
                sequences_query = """
                SELECT 
                    sequence_name,
                    data_type,
                    start_value,
                    minimum_value,
                    maximum_value,
                    increment,
                    cycle_option
                FROM information_schema.sequences
                WHERE sequence_schema = %s
                """
                
                await cursor.execute(sequences_query, (schema_name,))
                sequence_results = await cursor.fetchall()
                
                for seq_name, data_type, start_val, min_val, max_val, increment, cycle_option in sequence_results:
                    schema_info["sequences"][seq_name] = {
                        "data_type": data_type,
                        "start_value": str(start_val),
                        "minimum_value": str(min_val),
                        "maximum_value": str(max_val),
                        "increment": str(increment),
                        "cycle_option": cycle_option == 'YES'
                    }
                
                # Extract indexes using PostgreSQL system tables
                indexes_query = """
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = %s
                """
                
                await cursor.execute(indexes_query, (schema_name,))
                index_results = await cursor.fetchall()
                
                for schema, table_name, index_name, index_def in index_results:
                    schema_info["indexes"][index_name] = {
                        "table": table_name,
                        "definition": index_def
                    }
                
                # Extract detailed index information
                detailed_indexes_query = """
                SELECT 
                    i.relname as index_name,
                    t.relname as table_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary,
                    ix.indisexclusion as is_exclusion,
                    ix.indimmediate as is_immediate,
                    ix.indisclustered as is_clustered,
                    ix.indisvalid as is_valid,
                    am.amname as access_method,
                    array_agg(a.attname ORDER BY a.attnum) as columns
                FROM pg_index ix
                JOIN pg_class i ON ix.indexrelid = i.oid
                JOIN pg_class t ON ix.indrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                JOIN pg_am am ON i.relam = am.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE n.nspname = %s
                GROUP BY i.relname, t.relname, ix.indisunique, ix.indisprimary, 
                         ix.indisexclusion, ix.indimmediate, ix.indisclustered, 
                         ix.indisvalid, am.amname
                """
                
                await cursor.execute(detailed_indexes_query, (schema_name,))
                detailed_index_results = await cursor.fetchall()
                
                for index_name, table_name, is_unique, is_primary, is_exclusion, is_immediate, is_clustered, is_valid, access_method, columns in detailed_index_results:
                    if index_name in schema_info["indexes"]:
                        schema_info["indexes"][index_name].update({
                            "is_unique": is_unique,
                            "is_primary": is_primary,
                            "is_exclusion": is_exclusion,
                            "is_immediate": is_immediate,
                            "is_clustered": is_clustered,
                            "is_valid": is_valid,
                            "access_method": access_method,
                            "columns": list(columns) if columns else []
                        })
                
                # Extract check constraints
                check_constraints_query = """
                SELECT 
                    tc.table_name,
                    tc.constraint_name,
                    cc.check_clause
                FROM information_schema.table_constraints tc
                JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name
                WHERE tc.table_schema = %s 
                    AND tc.constraint_type = 'CHECK'
                """
                
                await cursor.execute(check_constraints_query, (schema_name,))
                constraint_results = await cursor.fetchall()
                
                for table_name, constraint_name, check_clause in constraint_results:
                    schema_info["constraints"][constraint_name] = {
                        "table": table_name,
                        "type": "CHECK",
                        "definition": check_clause
                    }
                
                # Extract unique constraints
                unique_constraints_query = """
                SELECT 
                    tc.constraint_name,
                    tc.table_name,
                    string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'UNIQUE' 
                    AND tc.table_schema = %s
                GROUP BY tc.constraint_name, tc.table_name
                """
                
                await cursor.execute(unique_constraints_query, (schema_name,))
                unique_results = await cursor.fetchall()
                
                for constraint_name, table_name, columns in unique_results:
                    schema_info["constraints"][constraint_name] = {
                        "table": table_name,
                        "type": "UNIQUE",
                        "columns": columns
                    }
                
                # Extract functions and procedures
                functions_query = """
                SELECT 
                    r.routine_name,
                    r.routine_type,
                    r.data_type as return_type,
                    r.routine_definition,
                    r.external_language,
                    r.security_type,
                    r.sql_data_access,
                    r.is_deterministic,
                    p.prosrc as source_code
                FROM information_schema.routines r
                LEFT JOIN pg_proc p ON r.routine_name = p.proname
                LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE r.routine_schema = %s
                    AND (n.nspname = %s OR n.nspname IS NULL)
                """
                
                await cursor.execute(functions_query, (schema_name, schema_name))
                function_results = await cursor.fetchall()
                
                for routine_name, routine_type, return_type, routine_definition, external_language, security_type, sql_data_access, is_deterministic, source_code in function_results:
                    schema_info["functions"][routine_name] = {
                        "type": routine_type,
                        "return_type": return_type,
                        "definition": routine_definition or source_code,
                        "language": external_language,
                        "security_type": security_type,
                        "sql_data_access": sql_data_access,
                        "is_deterministic": is_deterministic == 'YES'
                    }
                
                # Extract triggers
                triggers_query = """
                SELECT 
                    t.trigger_name,
                    t.event_manipulation,
                    t.event_object_table,
                    t.action_statement,
                    t.action_timing,
                    t.action_orientation
                FROM information_schema.triggers t
                WHERE t.trigger_schema = %s
                """
                
                await cursor.execute(triggers_query, (schema_name,))
                trigger_results = await cursor.fetchall()
                
                for trigger_name, event_manipulation, event_object_table, action_statement, action_timing, action_orientation in trigger_results:
                    if trigger_name not in schema_info["functions"]:
                        schema_info["functions"][trigger_name] = {}
                    
                    # Store trigger info in functions section with special type
                    schema_info["functions"][trigger_name] = {
                        "type": "TRIGGER",
                        "event": event_manipulation,
                        "table": event_object_table,
                        "timing": action_timing,
                        "orientation": action_orientation,
                        "statement": action_statement
                    }
        
        return json.dumps(schema_info, indent=2, default=str)
                
    except psycopg.Error as e:
        raise psycopg.Error(f"Database error: {e}")
    except json.JSONEncodeError as e:
        raise json.JSONEncodeError(f"JSON serialization error: {e}")

# Example usage
if __name__ == "__main__":
    import asyncio
    
    # Example connection string
    conn_str = "postgresql://username:password@localhost:5432/database_name"
    
    # Synchronous usage
    try:
        schema_json = extract_postgres_schema(conn_str, 'public')
        print("Synchronous extraction completed")
        print(schema_json[:200] + "..." if len(schema_json) > 200 else schema_json)
        
        # Optionally save to file
        with open('postgres_database_schema.json', 'w') as f:
            f.write(schema_json)
            
    except Exception as e:
        print(f"Error extracting schema: {e}")
    
    # Asynchronous usage
    async def async_example():
        try:
            schema_json = await extract_postgres_schema_async(conn_str, 'public')
            print("Asynchronous extraction completed")
            print(schema_json[:200] + "..." if len(schema_json) > 200 else schema_json)
            
            # Optionally save to file
            with open('postgres_database_schema_async.json', 'w') as f:
                f.write(schema_json)
                
        except Exception as e:
            print(f"Error extracting schema asynchronously: {e}")
    
    # Run async example
    asyncio.run(async_example())
    
    # Example of concurrent schema extraction from multiple databases
    async def extract_multiple_schemas():
        connection_strings = [
            "postgresql://user:pass@localhost:5432/db1",
            "postgresql://user:pass@localhost:5432/db2", 
            "postgresql://user:pass@localhost:5432/db3"
        ]
        
        # Extract schemas concurrently
        tasks = [
            extract_postgres_schema_async(conn_str, 'public') 
            for conn_str in connection_strings
        ]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Database {i+1} failed: {result}")
                else:
                    print(f"Database {i+1} schema extracted successfully")
                    # Save each schema to a separate file
                    with open(f'schema_db_{i+1}.json', 'w') as f:
                        f.write(result)
                        
        except Exception as e:
            print(f"Error in concurrent extraction: {e}")
    
    # Run concurrent extraction example
    # asyncio.run(extract_multiple_schemas())