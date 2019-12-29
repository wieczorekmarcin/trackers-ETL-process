# Ranking of the most popular trackers (ETL process)
## The goal of the task is to write an application that:
- reads data from files containing data on artists, their works and listening,
- will transform data in such a way that it can be placed in a database (e.g. SQlite),
- writes information to the standard output, such as: 
-- the artist with the most number of listenings
-- the 5 most popular songs and the time of data processing.

## Data source:
- file unique_tracks.txt - contains information such as song ID, performance ID, artist name and song title and can be downloaded from http://softmaz.net/unique_tracks.zip ,
- file triplets_sample_20p.txt - contains user ID, song ID and date of listening and can be downloaded from http://softmaz.net/triplets_sample_20p.zip ,
