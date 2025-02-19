def diff_table_versions(table_name: str, previous_timestamp: str, key_column: str, join_columns: list, compare_columns: list):
    """
    Compare the current version of a Delta table with a previous version based on a timestamp,
    using a composite join condition that includes a key column and additional columns.
    
    Parameters:
      table_name (str): Name of the Delta table.
      previous_timestamp (str): Timestamp string to reference the previous version (e.g., '2024-10-18T00:00:00.000Z').
      key_column (str): Primary key column used for joining (e.g., 'account_num').
      join_columns (list): List of additional column names to append to the join condition (e.g., ['amua_id_sf']).
      compare_columns (list): List of column names to compare between versions.
      
    Returns:
      A Spark DataFrame containing the differences between the two versions.
    """
    
    # Build join condition: Always join on the key_column and then append additional join columns if provided.
    join_conditions = [f"c.{key_column} = p.{key_column}"]
    if join_columns:
        join_conditions.extend([f"c.{col} = p.{col}" for col in join_columns])
    join_condition_str = " AND ".join(join_conditions)
    
    # Build diff conditions for the columns to compare
    diff_conditions = " OR ".join([f"c.{col} <> p.{col}" for col in compare_columns])
    
    # Build select clause for the compare columns, showing current and previous values.
    compare_selects = ",\n        ".join([f"c.{col} AS {col}_curr, p.{col} AS {col}_prev" for col in compare_columns])
    
    query = f"""
    WITH current_data AS (
      SELECT * FROM {table_name}
    ),
    previous_data AS (
      SELECT * FROM {table_name} TIMESTAMP AS OF '{previous_timestamp}'
    )
    SELECT 
        COALESCE(c.{key_column}, p.{key_column}) AS {key_column},
        {compare_selects},
        CASE
          WHEN c.{key_column} IS NULL THEN 'Missing in current'
          WHEN p.{key_column} IS NULL THEN 'New in current'
          WHEN {diff_conditions} THEN 'Value changed'
          ELSE 'No change'
        END AS diff_status
    FROM current_data c
    FULL OUTER JOIN previous_data p
      ON {join_condition_str}
    WHERE 
      c.{key_column} IS NULL 
      OR p.{key_column} IS NULL 
      OR {diff_conditions}
    """
    
    # Optional: Print the generated query for debugging.
    print("Generated SQL Query:\n", query)
    
    # Execute the query and return the resulting DataFrame.
    return spark.sql(query)