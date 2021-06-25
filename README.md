## The etl.py script runs without errors.

The script, etl.py, runs in the terminal without errors. The script reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.

## Analytics tables are correctly organized on S3.

Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

## The correct data is included in all tables.

Each table includes the right columns and data types. Duplicates are addressed where appropriate.


# schema

Spark DataFrames schemas are defined as a collection of typed columns. The entire schema is stored as a StructType and individual columns are stored as StructFields.

### Apache Parquet Introduction

Apache Parquet is a columnar file format that provides optimizations to speed up queries and is a far more efficient file format than CSV or JSON, supported by many data processing systems.




### Spark Write DataFrame to Parquet file format

Using spark.write.parquet() function we can write Spark DataFrame to Parquet file.


### Spark parquet partition – Improving performance

> Partitioning is a feature of many databases and data processing frameworks and it is key to make jobs work at scale. We can do a parquet file partition using spark partitionBy function.

> Parquet Partition creates a folder hierarchy for each spark partition; we have mentioned the first partition as year followed by artist_id hence, it creates a artist_id folder inside the year folder.
