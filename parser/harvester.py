# GeoTwitter Harvester
import mysql.connector
from mysql.connector import errorcode
from twython import TwythonStreamer, Twython, TwythonError
from xml.etree.ElementTree import ElementTree
from datetime import datetime

class Harvester:
	def __init__(self, config_file_name='config.xml'):
		tree = ElementTree()
		tree.parse(config_file_name)
		self.config = tree.getroot()

	def connect_to_twitter_stream(self):
		APP_KEY = self.config.find("./twitter/appkeys/app_key").text
		APP_SECRET = self.config.find("./twitter/appkeys/app_secret").text
		OAUTH_TOKEN = self.config.find("./twitter/appkeys/oauth_token").text
		OAUTH_TOKEN_SECRET = self.config.find("./twitter/appkeys/oauth_token_secret").text
		self.stream = ParserStream(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
		self.stream.db_cursor = self.db_connection.cursor()
		
	def connect_to_database(self):
		HOST = self.config.find("./database/host").text
		DATABASE_NAME = self.config.find("./database/database_name").text
		USER_NAME = self.config.find("./database/user_name").text
		PASSWORD = self.config.find("./database/password").text
		try:
			self.db_connection = mysql.connector.connect(host=HOST,database=DATABASE_NAME,user=USER_NAME,password=PASSWORD)
		except mysql.connector.Error as err:
			print("Database connection error. Error code: {}".format(err))
		
class ParserStream(TwythonStreamer):
	def on_success(self, data):
		if 'user' in data:
			check_tweet = "SELECT tweet_id FROM tw_tweet WHERE tweet_id = %s"
			self.db_cursor.execute(check_tweet % data['id'])
			if self.db_cursor.fetchone() is None:
				check_user = "SELECT user_id FROM tw_user WHERE user_id = %s"
				self.db_cursor.execute(check_user % data['user']['id'])
				if self.db_cursor.fetchone() is None:
					add_user = "INSERT INTO tw_user (user_id, username, screen_name, profile_image_url) VALUES (%s, %s, %s, %s)"
					user_data = (data['user']['id'],
						data['user']['name'],
						data['user']['screen_name'],
						data['user']['profile_image_url'])
					self.db_cursor.execute(add_user, user_data)
				if data['coordinates'] is None:
					add_tweet = "INSERT INTO tw_tweet (user_id, tweet_id, tweet_date, tweet_text, place_latitude, place_longtitude, place_full_name) VALUES (%s, %s, %s, %s, %s, %s, %s)"
					tweet_data = (data['user']['id'],
						data['id'],
						datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y"),
						data['text'],
						(data['place']['bounding_box']['coordinates'][0][0][0] + data['place']['bounding_box']['coordinates'][0][2][0])/2,
						(data['place']['bounding_box']['coordinates'][0][0][1] + data['place']['bounding_box']['coordinates'][0][2][1])/2,
						data['place']['country']+', '+data['place']['full_name'])
				else:
					add_tweet = "INSERT INTO tw_tweet (user_id, tweet_id, tweet_date, tweet_text, place_latitude, place_longtitude) VALUES (%s, %s, %s, %s, %s, %s)"
					tweet_data = (data['user']['id'],
						data['id'],
						datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y"),
						data['text'],
						data['coordinates']['coordinates'][0],
						data['coordinates']['coordinates'][1])
				self.db_cursor.execute(add_tweet, tweet_data)
				self.db_cursor.execute('COMMIT')
		#else : print(data['limit']['track'])

	def on_error(self, status_code, data):
		print (status_code)
		self.disconnect()