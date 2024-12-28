def row_iterator(df, column_name: str) -> str:
    # Collect the rows from the DataFrame
    rows = [row[column_name] for row in df.collect()]
    
    # Join the values into a comma-separated string
    row_string = ", ".join(f"'{id}'" for id in rows)
    
    return row_string