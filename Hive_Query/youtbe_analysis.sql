/* drop the table if exists */
DROP TABLE IF EXISTS youtubedata;
/*create the table if not exists */
CREATE TABLE if not exists youtubedata (
vid string, 
upldr string,
interval1 int,
category string,
length int, 
numview int,
rating float,
numratings int,
numcomment int,
relvids string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS textfile;
/* to show the Schema of this table */
DESCRIBE youtubedata;
/*load data hdfs inpath */
LOAD DATA INPATH '/user/kaushikdey67edu/youtubedata.txt' INTO TABLE youtubedata;
/* select the table */
SELECT * FROM youtubedata LIMIT 4;
/*Find out the top 5 categories with maximum number of videos uploaded.*/
Select category, count (*) as x from youtubedata group by category sort by x desc limit 5;
/*Find out the top 10 rated videos.*/
Select vid, rating from youtubedata sort by rating desc limit 10;
/*Find out the most viewed videos.*/
Select vid, numview from youtubedata sort by numview desc limit 100;