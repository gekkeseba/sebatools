from pyspark.sql import SparkSession

def check_duplicates(table_name, columns):
    spark = SparkSession.getActiveSession()
    if not spark:
        spark = SparkSession.builder.getOrCreate()
    df = spark.table(table_name)
    duplicates = df.groupBy(columns).count().filter("count > 1")
    duplicate_entries = df.join(duplicates, on=columns, how='inner')
    display(duplicate_entries)
    return duplicates.count() > 0