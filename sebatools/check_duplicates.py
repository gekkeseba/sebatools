def check_duplicates(spark, table_name, columns):
    df = spark.table(table_name)
    duplicates = df.groupBy(columns).count().filter("count > 1")
    duplicate_entries = df.join(duplicates, on=columns, how='inner')
    return duplicate_entries