# Spark SQL Converter

Converter Json data to Spark SQL query based on DataBase schema you upload. 

It works in two modes: 

    1. Output is printed in python console.
    2. Output is saved to file in given path. 

### Synopsis

```bash
python3 file.py --schema_file_path file1.csv --data_file_path file2.json [--query_file_path] file3.txt
```

### Options
```bash
*schema_file_path*     string value with location of DataBase schema.

*data_file_path*       string value with location of Json file.
 
 query_file_path       string value with location where output query will be saved. INCLUDE name of file and extension.

** is required 
```

## Usage
To get your output printed in console give only required arguments:
```bash
python3 spark-sql-converter.py --schema_file_path --data_file_path 
```
To get your output saved in file give also not required arguments:
```bash
python3 spark-sql-converter.py --schema_file_path --data_file_path --query_file_path
```

## Examples
Print output in console:
```bash
python3 spark-sql-converter.py --schema_file_path "/Users/abc/Desktop/schema.csv" --data_file_path "/Users/abc/Desktop/data.json" 
```
Save output in txt file:
```bash
python3 spark-sql-converter.py --schema_file_path "/Users/abc/Desktop/schema.csv" --data_file_path "/Users/abc/Desktop/data.json" --query_file_path "/Users/abc/Desktop/sql_query"
```