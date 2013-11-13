CREATE DATABASE geotwitter;

USE geotwitter;

CREATE TABLE  tw_user 
(
   user_id bigint NOT NULL,
   last_tweet_id bigint NOT NULL,
   last_statuses_count bigint NOT NULL,
   last_update_time int NOT NULL,
   priority int NOT NULL,
   username varchar(20) NOT NULL,
   screen_name varchar(20) NOT NULL,
   profile_image_url varchar(255) NOT NULL,
   PRIMARY KEY(user_id) 
);

CREATE TABLE tw_tweet 
( 
	tweet_id bigint NOT NULL, 
	tweet_text varchar(140), 
	tweet_date timestamp NOT NULL, 
	place_latitude float(10,7) NOT NULL,
	place_longtitude float (10,7) NOT NULL,
	place_full_name varchar(255),
	user_id bigint NOT NULL references tw_user(user_id), 
	PRIMARY KEY(tweet_id) 
);