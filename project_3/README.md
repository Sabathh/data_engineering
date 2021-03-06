# Project: Data Warehouse

Third project of the Data Engineering Nanodegree.

## Project description

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## How to Execute

### Creating Redshift Cluster

Run the notebook [create_cluster.ipynb](https://github.com/Sabathh/data_engineering/blob/master/project_3/create_cluster.ipynb) to generate a Redshift cluster with the expected configurations for this project. The configuration file [dwh.cfg](https://github.com/Sabathh/data_engineering/blob/master/project_3/dwh.cfg) must be completed with the user's AWS credentials before running

```text
[AWS]
KEY=
SECRET=
```

### Creating tables in Redshift

Run [create_tables.py](https://github.com/Sabathh/data_engineering/blob/master/project_3/create_tables.py) to create the database tables in the Redshift cluster. Details about the tables schema is available at [Database Schemas](#database-schemas) section.

### Populating tables

Run [etl.py](https://github.com/Sabathh/data_engineering/blob/master/project_3/etl.py) to populate the tables in Redshidt with the data from the datasets.

## Datasets

### Song Dataset

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. Here's an example of the format of a single song file:

```json
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

Here's an example of the format of log file:

![Example of Log Dataset](https://github.com/Sabathh/data_engineering/blob/master/project_1/images/log-data.png?raw=true "Example of Log Dataset")

## Database Schemas

The schema model selected for this exercise is the Star Schema. It contains 1 Fact Table with the measures associated to each event, and 4 Dimensional Tables, each with a Primary Key referenced from the Fact Table.

### Fact Table

#### songplays

- songplay_id (int PRIMARY KEY) : Unique ID of each song played
- start_time (float) : Timestamp of beginning of user activity
- user_id (int) : Unique User ID
- level (varchar) : User level (free/paid)
- song_id (varchar) : Unique song ID
- artist_id (varchar) : Unique artist ID
- session_id (int) : Unique user session ID
- location (varchar) : User location
- user_agent (varchar) : Platform used by the user to access service

### Dimension Tables

#### users

- user_id (int PRIMARY KEY) : Unique User ID
- first_name (varchar) : User's first name
- last_name (varchar) : User's last name
- gender (varchar) : User's gender
- level (varchar) : User level (free/paid)

#### songs

- song_id (varchar PRIMARY KEY) : Unique song ID
- title (varchar) : Song title
- artist_id (varchar) : Unique artist ID
- year (int) : Year the song was released
- duration (float) : Song duration

#### artists

- artist_id (varchar PRIMARY KEY) : Unique artist ID
- name (varchar) : Artist's name
- location (varchar) : Artist's city
- latitude (float) : Artist's latitude
- longitude (float) : Artist's longitude

#### time

- start_time (float PRIMARY KEY) : Timestamp of beginning of user activity
- hour (int) : Hour related to start_time
- day (int) : Day related to start_time
- week (int) : Week related to start_time
- month (int) : Month related to start_time
- year (int) : Year related to start_time
- weekday (varchar) : Weekday related to start_time