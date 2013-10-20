# GeoTwitter Harvester

class Harvester:
	def __init__(self, config_file_name='config.xml'):
		import xml.etree.ElementTree as ET
		self.config = ET.parse(config_file_name).getroot()

	def connect_to_twitter(self):
		from twython import Twython, TwythonError
		APP_KEY = self.config.find("./twitter/appkeys/app_key").text
		APP_SECRET = self.config.find("./twitter/appkeys/app_secret").text
		OAUTH_TOKEN = self.config.find("./twitter/appkeys/oauth_token").text
		OAUTH_TOKEN_SECRET = self.config.find("./twitter/appkeys/oauth_token_secret").text
		self.twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

	def connect_to_database(self):
		import psycopg2
		DATABASE_NAME = self.config.find("./database/database_name").text
		USER_NAME = self.config.find("./database/user_name").text
		self.database = psycopg2.connect("dbname={} user={}".format(DATABASE_NAME, USER_NAME))

	def parse_twitter_user(self,user_screen_name):
		try:
			user_timeline = self.twitter.get_user_timeline(screen_name=user_screen_name)
		except TwythonError as e:
			print (e)
	
	