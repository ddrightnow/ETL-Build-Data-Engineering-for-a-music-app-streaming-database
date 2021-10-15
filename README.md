# Project: Connect Sparkify song database to ETL pipeline and process the data into incoming tables



## Project Goals: 
Extract data from 2 local files into different tables for segmentation and analysis of songs users play.
Store song, artist, and user table in normalized table formats for more efficient data manipulation.
Analyse user data via songs played



## 3 processes:
Extract data from local data files
Transform the data into acceptable table data formats
Load into PostgreSQL tables



## Database Schema Design
5 main Fact/Dimension tables:
**Songplay table:** House the songs played along with the artist and user data. Artist and user data is needed as this helps to identify which user was playing what song. 

**User table:** Houses user ID, user names and gender. A normalized table to keep data in it simplest form. The user ID helps for effective identification of the users.

**Song table:** Houses song data in a normalized format for effective data manipulation. Songs identifiable by ID

**Artist table:** Houses artist data in a normalized format for effective data manipulation. Artists identifiable by ID

**Time table:** Houses time data in timestamp format, and the timestamp breakdown into different lower hierarchal time units. These lower timeframe units will help users to dice, slice or recategorize data as needed. 



## ETL Pipeline:
-	Load the data from the song_data file. It contains both song and artist data.
-	Filter data based on song related fields and insert into song table
-	Filter data based on artist related fields and insert into artist table
-	Load the data from the log_data file. It contains both time and user data. The data describes songs users played and at what time (in milliseconds)
-	Filter records by NextSong action
-	Convert the timestamp data to milliseconds and add columns describing the timestamp information in lower timeframe formats, and insert these into time table.
-	Filter data based on user related fields and insert into user table
-	Query database for table displaying both song ID and artist ID data, using the artist ID as a column to join the data. 
-	Add both song ID and artist ID data along with song play data into songplay table.
-	Close the connection



## File Execution
Run the etl.ipynb file in chronological order from top to bottom.
Each section starts and completes an stage in the ETL process

### ETL Process:
Imports python file dependencies, creates and connects to Sparkify database 

### Process Song data
Adds song and artist data to their respective table

### Process Log data
Adds time, users and songplays data to their respective tables

### Close Connection to Sparkify Database
Close connection to the database and ends ETL process



## Project Documents

### etl.py
Houses the implementation code for ETL process for the entire dataset

### create_tables.py
Houses function definitions for creating/dropping each table using the queries in `create_table_queries` list. 
Houses function definitions for Creates and connects to the sparkifydb & Returns the connection and cursor to sparkifydb

### sql_queries.py
Houses the table creation, table selection and table insert SQL queries

### test.ipynb
File to test the tables were 1) correctly created and 2) data properly entered