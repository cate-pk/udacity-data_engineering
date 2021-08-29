# Project : Data Modeling with Postgres
---

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Resources
**Datasets**
- Song dataset
- Log dataset

**Tables (a star schema)**
- **Fact Table**
1. songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- **Dimension Tables**
2. users - users in the app
user_id, first_name, last_name, gender, level
3. songs - songs in music database
song_id, title, artist_id, year, duration
4. artists - artists in music database
artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

![diagram](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/339318/1586016120/Song_ERD.png)

## Project Steps
- Create Tables : sql_queries.py, create_tables.py
- Build ETL Processes : etl.ipynb
- Build ETL Pipeline : etl.ipynb, etl.py

---

## ELT Processes
* All the queries are in the sql_queries.py (including DROP, CREATE, INSERT, and FIND)
### Processing Song Data
[Table] songs
- Selected columns : song ID, title, artist ID, year, and duration 
- From the columns above, using `df.values`, values were added to the table

[Table] artists
- Selected columns : artist ID, name, location, latitude, and longitude
- From the columns above, using `df.values`, values were added to the table

### Processing Log Data
[Table] time
- Filter records by `NextSong` action
- Convert to datetime columns (in milliseconds) : 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'
- The table containing such data has been made to pd.DataFrame
    ```python
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday], axis = 1)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data.values, columns=column_labels)```

[Table] users
- Selected columns : user ID, first name, last name, gender and level 
- To maintain that each users are unique, the users were tested from duplicates and NULL values
    ```python
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates().dropna()
    ```

[Table] songplays
- Source : songs table, artists table, and original log file
- Selected columns : pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent
- From the DataFrame of the Log Data, the data was reviewed iteratively by rows for information on the selected columns above.
```python
for index, row in df.iterrows():
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()
```