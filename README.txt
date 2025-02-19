Python package which has several functions for data transformations
1. remove emoji's from any string of text
2. row_iterator; iterates over rows and puts it in the format of: 'x','y','z'. 
3. check_duplicates; checks for duplicate columns in a dataframe. Can handle multiple columns to perform a duplicate check on.
4. diff_table_version; checks data between 2 versions of the same table.
Expects: table name, timestamp of version, key column (first column to join on), join columns (additional columns to join on), columns to compare differences.  be done based on 