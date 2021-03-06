## Sparkify DWH and ETL

Sparkify has a requirement to analyze user activity on their new streaming music app. Their information of interest is stored in JSON files in an S3 Bucket for both the song metadata and user activity. 


They want to do analytics on the songs played and thus a Cloud Based Data Warehouse on Amazon Redshift with appropriate tables along with an ETL job has been created to meet this requirement.


### Dataset:
#### Song Dataset
The song dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

#### Log Dataset
The logdataset consists of log files in JSON format generated by this  [event simulator](https://github.com/Interana/eventsim)  based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations. The log files in the dataset are partitioned by year and month.

### Tables:

#### Staging Events Table
The staging_events table is a staging table created to store the log data set in the AWS Redshift DataWarehouse. A Copy Command is executed which copies the file data exactly as is from the S3 bucket into this table. Compression is turned off and a JSONPath file is provided to specify the schema of the table.

#### Staging Songs Table
The staging_songs table is a staging table created to store the Song Data set.in the AWS Redshift DataWarehouse. A Copy Command is executed which copies the file data exactly as is from the S3 bucket into this table. Compression is turned off during this process to speed up the copy process.

#### Songplays Table
The songplays table is the Fact Table and consists of records in log data associated with song plays i.e. records with page `NextSong`. The attributes are _songplay_id, start_time, userId, level, song_id, artist_id, session_id, location, user_agent_.  The *songplay_id* is set as an auto-incrementing variable, the *start_time* is set as a TIMESTAMP, the *session_id* being an INTEGER and the *user_agent* being TEXT since it can be very long. All the other attributes are set to VARCHAR.The song_id is kept as distribution key since the songs are the largest dimension and the start_time is kept as the sort key since it is expected that the queries will be sorted using that variable.

#### Users Table
The Users Table is a dimension table consists of the *userId , firstname, lastname, gender* and *level*. All the attributes are alphanumeric in nature and have thus been set to VARCHAR or CHAR in the table.The distribution style is set to all since it is relatively very small in size and this can help with queries involving their attributes and avoid broadcasting across Redshift nodes.

#### Songs Table
 The songs Table is a dimension table and consists of *songId, Title, artistID, Year* and *duration*. INTEGER data type is a good fit for the *Year* field and the *duration* field is a floating point number and is therefore set to a float type in the DW. The others are all set to VARCHAR since they are alphanumeric. Since the joins are expected to be on the song_id, it is set as the distribution key as the table is relative larger in size compared to the users dimension table.
 
#### Artists Table
The Artists  Table is a dimension table and consists of the *ArtistID, Name, Location , the Latitude* and *Longitude*. The *Latitude* and *Longitude* are floating point variables and have been set to float. The others are alphanumeric and are thus set to VARCHAR.Since the joins are expected to be on the artist_id, it is set as the distribution key as the table is relative larger in size compared to the users dimension table.

 #### Time Table
 The Time Table is a dimension table which consists of timestamps of records in **songplays** broken down into specific units. The attributes are  *start_time, hour, day, week, month, year*, and *weekday*. The *start_time* is set to a TIMESTAMP  the *weekday* and *month* to a TEXT and the rest are non fractional numbers are a good fit for INTEGER data type.Since the joins are expected to be on the start_time and be sorted on that attribute, it is set as the distribution and sort key as the table is relative larger in size compared to the users dimension table.
 
### ETL Approach
The ETL strategy consists of two steps. First the events and the songs data are loaded from the S3 bucket into the Staging Tables with the compression turned off to speed up the loading process. It is assumed that the Redshift table is ready and accepting connections. Subsequently, an ETL process is undertaken to populate the fact and dimension tables by reading the information out of the staging tables, processing them and inserting them into the desired tables.

### Files
There are 5 python files applicable to this project. The *sql_queries.py* file has all the queries to create, update and drop the DW tables. The *create_tables.py* module consists of the scripts to drop and recreate all the tables. The *etl.py* is the file that undertakes the ETL job of reading the data files and running the ETL job to store the data into the appropriate tables. The *dwh.cfg* file consists of configuration information e.g. location of the S3 buckets, the DW hostname etc. The *Test.ipynb* is a sample accompanying python notebook where the python modules are run and results for a sample analysis query  and the response is shown.

### Scripts
To run the scripts, please ensure that there is an existing connection to a Redshift DWH and the appropriate information populated in the config file. Then run the create_tables.py script first to drop existing tables and create new ones. Subsequently run the etl.py script to load the the data into the tables in the DWH.
