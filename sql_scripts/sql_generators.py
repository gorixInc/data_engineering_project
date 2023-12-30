from sqlalchemy.inspection import inspect
def create_deduplication_sql(schema, table, unique_columns, fk_tables, fk_columns):
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
    
    update_fk_sqls = []
    # SQL to update foreign key tables
    for i, fk_table in enumerate(fk_tables):
        fk_column = fk_columns[i]

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
        update_fk_sqls.append(update_fk_sql)
    
    update_fk_sql_full = '\n'.join(update_fk_sqls)
    
    # SQL to delete duplicates
    delete_duplicates_sql = f"DELETE FROM {schema}.{table} WHERE {temp_column};\n"
    
    # SQL to drop the temporary column
    drop_column_sql = f"ALTER TABLE {schema}.{table} DROP COLUMN {temp_column};"

    return ''.join([add_column_sql, mark_duplicates_sql, update_fk_sql_full, delete_duplicates_sql, drop_column_sql])

def append_to_schema(source, target, table_classes, start_time, exclude_col='processed_at'):
    commands = []
    for table_class in table_classes:
        table_name = table_class.__tablename__
        column_names = [column.name for column in inspect(table_class).c if not column.name == exclude_col]
        c = f"""
        ALTER TABLE {target}.{table_name} DISABLE TRIGGER ALL;
        INSERT INTO {target}.{table_name} ({','.join(column_names)})
        SELECT {','.join(column_names)} FROM {source}.{table_name} 
        WHERE processed_at = '{start_time}';
        ALTER TABLE {target}.{table_name} ENABLE TRIGGER ALL;
        """
       # SELECT setval('{target}.{table}_id_seq', (SELECT MAX(id) + 1 FROM {source}.{table}));
        commands.append(c)
    return "\n".join(commands)

def truncate_tables(schema, tables):
    commands = []
    for table in tables:
        c = f"TRUNCATE TABLE {schema}.{table} CASCADE;"
        commands.append(c)
    return '\n'.join(commands)