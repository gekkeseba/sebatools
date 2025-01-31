def check_duplicates(spark, table_name, columns):
    df = spark.table(table_name)
    duplicates = df.groupBy(columns).count().filter("count > 1")
    duplicate_entries = df.join(duplicates, on=columns, how='inner')
    
    # Check if the display function is available
    if 'display' in globals():
        display(duplicate_entries)
    else:
        duplicate_entries.show()  # Use show() as a fallback
    
    return duplicates.count() > 0