def create_deduplication_sql(schema, table, unique_columns, fk_table, fk_column):
    unique_cols_str = ', '.join(unique_columns)
    temp_column = 'is_duplicate'
    
    # SQL to add a temporary column
    add_column_sql = f"ALTER TABLE {schema}.{table} ADD COLUMN {temp_column} BOOLEAN DEFAULT FALSE;"
    
    # SQL to mark duplicates
    mark_duplicates_sql = f"""
    UPDATE {schema}.{table}
    SET {temp_column} = TRUE
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM {schema}.{table}
        GROUP BY {unique_cols_str}
    );"""
    
    # SQL to update foreign key table
    update_fk_sql = f"""
    UPDATE {schema}.{fk_table} fk
    SET {fk_column} = (
        SELECT MIN(p.id)
        FROM {schema}.{table} p
        WHERE {' AND '.join([f'p.{col} = t.{col}' for col in unique_columns])}
        AND NOT p.{temp_column}
        AND fk.{fk_column} = t.id
    )
    FROM {schema}.{table} t
    WHERE fk.{fk_column} = t.id
    AND t.{temp_column};
    """
    
    # SQL to delete duplicates
    delete_duplicates_sql = f"DELETE FROM {schema}.{table} WHERE {temp_column};\n"
    
    # SQL to drop the temporary column
    drop_column_sql = f"ALTER TABLE {schema}.{table} DROP COLUMN {temp_column};"

    return ''.join([add_column_sql, mark_duplicates_sql, update_fk_sql, delete_duplicates_sql, drop_column_sql])

def append_to_schema(source, target, tables):
    commands = []
    for table in tables:
        c = f"""
        ALTER TABLE {target}.{table} DISABLE TRIGGER ALL;
        INSERT INTO {target}.{table}
        SELECT * FROM {source}.{table};
        ALTER TABLE {target}.{table} ENABLE TRIGGER ALL;
        """
        commands.append(c)
    return "\n".join(commands)

def truncate_tables(schema, tables):
    commands = []
    for table in tables:
        c = f"TRUNCATE TABLE {schema}.{table} CASCADE;"
        commands.append(c)
    return '\n'.join(commands)