# GeoTwitter Harvester
import mysql.connector, time
from mysql.connector import errorcode
from twython import TwythonStreamer, Twython, TwythonError
from xml.etree.ElementTree import ElementTree
from datetime import datetime

class Harvester:
	def __init__(self, config_file_name='config.xml'):
		tree = ElementTree()
		tree.parse(config_file_name)
		self.config = self.etree_to_dict(tree.getroot())
	
	def etree_to_dict(self, t):
		if len(t) > 0:
			return {i.tag: self.etree_to_dict(i) for i in t}
		else:
			return t.text
	
	def connect_to_database(self):
		HOST = self.config['database']['host']
		DATABASE_NAME = self.config['database']['database_name']
		USER_NAME = self.config['database']['user_name']
		PASSWORD = self.config['database']['password']
		try:
			self.db_connection = mysql.connector.connect(host=HOST,database=DATABASE_NAME,user=USER_NAME,password=PASSWORD)
		except mysql.connector.Error as e:
			print("Database connection error. Error code: {}".format(e))

	def connect_to_twitter_stream(self):
		self.stream = ParserStream(self.config['twitter'], self.db_connection.cursor())
	
	def connect_to_twitter_rest(self):
		self.rest = ParserREST(self.config['twitter'], self.db_connection.cursor())

class DBCursor:
	def __init__ (self, cursor):
		self.db_cursor = cursor
		self.add_user = "INSERT INTO tw_user (user_id, username, screen_name, profile_image_url, last_tweet_id, last_statuses_count, last_update_time, priority) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
		self.add_tweet = "INSERT INTO tw_tweet (user_id, tweet_id, tweet_date, tweet_text, place_longtitude, place_latitude, place_full_name) VALUES (%s, %s, %s, %s, %s, %s, %s)"
		self.update_user = "UPDATE tw_user SET last_tweet_id = %s, last_statuses_count = %s, last_update_time = %s, priority = %s WHERE user_id = %s"
		self.select_user = "SELECT last_tweet_id, last_statuses_count FROM tw_user WHERE user_id = %s"
		self.select_tweet = "SELECT user_id FROM tw_tweet WHERE tweet_id = %s"
		self.select_users_for_processing = "SELECT user_id FROM tw_user WHERE last_update_time < %s limit %s"
	
	def add_user_data(self, data):
		user_data = (data['id'],
			data['name'],
			data['screen_name'],
			data['profile_image_url'],
			1, 0, 0, 0)
		self.db_cursor.execute(self.add_user, user_data)
		self.db_cursor.execute('COMMIT')
	
	def add_tweet_data(self, data):
		if data['coordinates'] is None:
			tweet_data = (data['user']['id'],
				data['id'],
				datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y"),
				data['text'],
				(data['place']['bounding_box']['coordinates'][0][0][0] + data['place']['bounding_box']['coordinates'][0][2][0])/2,
				(data['place']['bounding_box']['coordinates'][0][0][1] + data['place']['bounding_box']['coordinates'][0][2][1])/2,
				data['place']['country'] + ', ' + data['place']['full_name'])
		else:
			tweet_data = (data['user']['id'],
				data['id'],
				datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y"),
				data['text'],
				data['coordinates']['coordinates'][0],
				data['coordinates']['coordinates'][1],
				'N/A')
		self.db_cursor.execute(self.add_tweet, tweet_data)
		self.db_cursor.execute('COMMIT')
	
	def get_parse_user_list(self, last_time, list_size):
		user_data = (int(time.time())-last_time, list_size)
		self.db_cursor.execute(self.select_users_for_processing, user_data)
		return self.db_cursor.fetchall()
	
	def update_user_statistic(self, data):
		user_data = (data['last_tweet_id'],
			data['last_statuses_count'],
			int(time.time()),
			0,
			data['user_id'])
		print(user_data)
		self.db_cursor.execute(self.update_user, user_data)
		self.db_cursor.execute('COMMIT')
		
	def select_user_statistic(self, user_id):
		self.db_cursor.execute(self.select_user % user_id)
		result = self.db_cursor.fetchone()
		if result is None:
			return None
		else:
			return {'last_tweet_id':result[0],'statuses_count':result[1]}
	
	def tweet_in_db(self, tweet_id):
		self.db_cursor.execute(self.select_tweet % tweet_id)
		return self.db_cursor.fetchone()
		

class ParserREST:
	def __init__ (self, config, db_cursor):
		self.twitter = Twython(config['appkeys']['app_key'],
			config['appkeys']['app_secret'],
			config['appkeys']['oauth_token'],
			config['appkeys']['oauth_token_secret'])
		self.db_cursor = DBCursor(db_cursor)
		self.timeline_statuses_max = int(config['user_timeline']['timeline_statuses_max'])
		self.timeline_limit_user = int(config['user_timeline']['limit_user'])
		self.page_size = int(config['user_timeline']['page_size'])
		self.time_window = float(config['time_window'])
		self.time_start = time.time()
		self.last_time = int(config['user_in_base']['timeout'])
		self.list_size = int(config['user_in_base']['list_limit'])
		try:
			data = self.twitter.get_application_rate_limit_status(resources = 'statuses')
			self.timeline_request_counter = data['resources']['statuses']['/statuses/user_timeline']['remaining']
		except TwythonError as e:
			self.timeline_request_counter = self.timeline_limit_user
			print ('Error in ParserREST.__init__')
			print (e)

	def parse(self):
		process = True
		while process:
			users_list = self.db_cursor.get_parse_user_list(last_time = self.last_time,
															list_size = self.list_size)
			if len(users_list) > 0:
				for i in users_list:
					self.parse_twitter_user(i[0])
			else:
				process = False
	
	def parse_twitter_user(self,user_id):
		print('user_id: {}'.format(user_id))
		try:
			statistic = self.db_cursor.select_user_statistic(user_id)
			if statistic is None:
				user = self.twitter.show_user(user_id=user_id)
				self.db_cursor.add_user_data(user)
				if user['statuses_count'] == 0:
					self.db_cursor.add_user_data({'last_tweet_id':1,
						'last_statuses_count':0,
						'user_id':user_id})
					return
				else:
					result = self.get_user_timeline(user_id, 1)
					self.db_cursor.update_user_statistic({'last_tweet_id':result['last_id'],
						'last_statuses_count':result['statuses_count'],
						'user_id':user_id})
			else:
				result = self.get_user_timeline(user_id, statistic['last_tweet_id'])
				self.db_cursor.update_user_statistic({'last_tweet_id':result['last_id'],
						'last_statuses_count':result['statuses_count'] + statistic['statuses_count'],
						'user_id':user_id})
		except Exception as e:
				print ('Error in ParserREST.parse_twitter_user')
				print (e)
				return
	
	def get_user_timeline(self, user_id, since_id):
		statuses_count = 0
		if since_id > 1:
			since_id_tmp = since_id - 1
		else:
			since_id_tmp = 1
		first = True
		process = True
		last_id = since_id
		while process:
			if (time.time() - self.time_start) > self.time_window:
				self.time_start = time.time()
				self.timeline_request_counter = self.timeline_limit_user
			if self.timeline_request_counter == 0:
				print('Have to wait.')
				time.sleep(self.time_window - (time.time() - self.time_start))
			else:
				self.timeline_request_counter = self.timeline_request_counter - 1
				if first:
					user_timeline = self.twitter.get_user_timeline(user_id = user_id, 
						count = self.page_size,
						since_id = since_id_tmp,
						exclude_replies = True,
						include_rts = False,
						trim_user = True)
				else:
					user_timeline = self.twitter.get_user_timeline(user_id = user_id, 
						count = self.page_size,
						since_id = since_id_tmp,
						max_id = max_id,
						exclude_replies = True,
						include_rts = False,
						trim_user = True)
				#print('len(user_timeline): {}'.format(len(user_timeline)))
				if len(user_timeline) > 0:
					if first:
						last_id = user_timeline[0]['id']
						first = False
					if user_timeline[-1]['id'] == since_id:
						del(user_timeline[-1])
						process = False
					for status in user_timeline:
						if not (status['coordinates'] is None and status['place'] is None):
							self.db_cursor.add_tweet_data(status)
							statuses_count = statuses_count + 1
					max_id = user_timeline[-1]['id'] - 1
				else:
					process = False
		return {'statuses_count':statuses_count,'last_id':last_id}

	def get_user_timeline_test(self, user_id, since_id):
		statuses_count = 0
		if since_id > 1:
			since_id_tmp = since_id - 1
		else:
			since_id_tmp = 1
		first = True
		process = True
		while process:
			if (time.time() - self.time_start) > self.time_window:
				self.time_start = time.time()
				self.timeline_request_counter = self.timeline_limit_user
			if self.timeline_request_counter == 0:
				self.timeline_request_counter = self.timeline_limit_user
			else:
				self.timeline_request_counter = self.timeline_request_counter - 1
				if first:
					user_timeline = [{'user':{'id':user_id,'name':'test_user','screen_name':'test_user','profile_image_url':'testurl'},
						'id':since_id + 1,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
						'text':str(time.time()),
						'coordinates':{'coordinates':[1, 1]}},
						{'user':{'id':user_id,'name':'test_user','screen_name':'test_user','profile_image_url':'testurl'},
						'id':since_id,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
						'text':str(time.time()),
						'coordinates':{'coordinates':[1, 1]}}]
					first = False
				else:
					user_timeline = [{'user':{'id':user_id,'name':'test_user','screen_name':'test_user','profile_image_url':'testurl'},
						'id':since_id + 1,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
						'text':str(time.time()),
						'coordinates':{'coordinates':[1, 1]}},
						{'user':{'id':user_id,'name':'test_user','screen_name':'test_user','profile_image_url':'testurl'},
						'id':since_id,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
						'text':str(time.time()),
						'coordinates':{'coordinates':[1, 1]}}]
				if len(user_timeline) > 0:
					if user_timeline[-1]['id'] == since_id:
						del(user_timeline[-1])
						process = False
					statuses_count = statuses_count + len(user_timeline)
					for status in user_timeline:
						if not (status['coordinates'] is None and status['place'] is None):
							self.db_cursor.add_tweet_data(status)
					max_id = user_timeline[-1]['id'] - 1
				else:
					process = False
		return {'statuses_count':statuses_count,'last_id':since_id}
	
class ParserStream(TwythonStreamer):
	def __init__ (self, config, db_cursor):
		TwythonStreamer.__init__(self,
			config['appkeys']['app_key'],
			config['appkeys']['app_secret'],
			config['appkeys']['oauth_token'],
			config['appkeys']['oauth_token_secret'])
		self.db_cursor = DBCursor(db_cursor)
		self.errors = config['errors']
		self.username_only = True;
		
	def parse(self, username_only = True):
		self.username_only = username_only;
		process = True
		counter = 0
		last_error_time = time.time()
		while process:
			try:
				self.statuses.filter(locations='-180,-90,180,90')
			except Exception as e:
				print("Twitter connection error. {}".format(e))
				print("Attempting to connect.")
				if time.time() - last_error_time > float(self.errors['connection']['timeout']) * float(self.errors['connection']['attempts']):
					counter = int(self.errors['connection']['attempts'])
					last_error_time = time.time()
				counter = counter - 1
				if counter == 0:
					print("Can not to connect.")
					process = False
				else:
					time.sleep(int(self.errors['connection']['timeout']))

	def test_data(self, start_user_id, tweet_id_step):
		data = {'user':{'id':start_user_id,'name':'test_user','screen_name':'test_user','profile_image_url':'testurl'},
			'id':start_user_id + tweet_id_step,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
			'text':'2013-10-29 00:11:07.875088',
			'coordinates':{'coordinates':[1, 1]}}
		while True:
			data['user']['id'] = data['user']['id'] + 1
			data['id'] = data['id'] + 1
			data['text'] = str(datetime.now())
			self.on_success(data)
			
	def on_success(self, data):
		if 'user' in data:
			if self.username_only:
				statistic = self.db_cursor.select_user_statistic(data['user']['id'])
				if statistic is None:
					self.db_cursor.add_user_data(data['user'])
			else:
				if self.db_cursor.tweet_in_db(data['id']) is None:
					statistic = self.db_cursor.select_user_statistic(data['user']['id'])
					if statistic is None:
						self.db_cursor.add_user_data(data['user'])
					self.db_cursor.add_tweet_data(data)

	def on_error(self, status_code, data):
		print ('Stream error code {}'.format(status_code))
		self.disconnect()	


