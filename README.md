# Data Engineering Nanodegree -- Project I

## Purpose

This project's purpose is to create an **ETL** pipeline:
* extract **(E)** the list of JSON files in the `data/` directory
* transform **(T)** it (to extract relevant information)
* and load **(L)** it into a PostgreSQL database

## Comands
You can create a conda env by calling:
```
conda create --name SOME_NAME --file requirements_conda.txt
```

To create the database and initilize all the tables into which the data is going to be loaded, run:
```
python create_tables.py
```

Once that's done, the ETL is run by calling:
```
python etl.py
```

## The files

### `sql_queries.py`
This file contains all queries (`CREATE DATABASE / TABLE`, `INSERT INTO`, `DROP TABLE`, etc.) that are executed (using `psycopg2`'s API) in the `create_tables.py` and `etl.py` scripts.

### `create_tables.py`
Contains functions that first create the *Sparkify* database (if it does not exist), drops any tables that might already be there, and creates them a new.

### `etl.py`
This is where the magic happens. There's a central `process_data()` function that reads in JSONs from either the `data/log_data` or the `data/song_data` directory, to then commit the transformed data.
For the actual reading in the files, transforming the data, and formulating queries for inserting rows into the tables, there are two dedicated functions: `process_log_file()` and `process_song_file()`.
