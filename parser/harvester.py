# GeoTwitter Harvester
import logging
import logging.handlers
import time
from copy import deepcopy
from datetime import datetime
from multiprocessing import Process, Queue, Pipe, current_process, freeze_support, cpu_count
from xml.etree.ElementTree import ElementTree
import mysql.connector
from mysql.connector import errorcode
from twython import TwythonStreamer, Twython, TwythonError

class Harvester:
    def __init__(self, config_file_name='config.xml'):
        tree = ElementTree()
        tree.parse(config_file_name)
        self.config = self.etree_to_dict(tree.getroot())
    
    def etree_to_dict(self, t):
        if t:
            return {i.tag: self.etree_to_dict(i) for i in t}
        else:
            return t.text
    
    def connect_to_database(self):
        self.db_cursor = DBCursor(self.config['database'])

    def connect_to_twitter_stream(self):
        config = deepcopy(self.config)
        config['twitter']['appkeys'] = self.config['twitter']['appkeys']['main']
        self.stream = ParserStream(config)
    
    def connect_to_twitter_rest(self):
        config = deepcopy(self.config)
        config['twitter']['appkeys'] = self.config['twitter']['appkeys']['main']
        self.rest = ParserREST(config)
        
    def probe_log_configurer(self, queue):
        h = logging.handlers.QueueHandler(queue)
        root = logging.getLogger()
        root.addHandler(h)
        root.setLevel(logging.DEBUG)
        
    def probe_logger(self, queue):
        logger = logging.getLogger('root.logger')
        h = logging.handlers.TimedRotatingFileHandler('Harvester.log', when='midnight')
        f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        filt = logging.Filter(name='root')
        h.setFormatter(f)
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.addFilter(filt)
        logger.info('Logger: Start')
        while True:
            try:
                record = queue.get()
                logger.handle(record)
                if record.name == 'root.cmd_stop':
                    break
            except:
                pass

    def probe_rest(self, config, task_queue, log_queue, control):
        self.probe_log_configurer(log_queue)
        logging.info('Start probe_rest id {}'.format(config['id']))
        probe = ParserREST(config)
        while True:
            try:
                if control.poll():
                    if control.recv() == 'Stop':
                        break
                probe.parse_twitter_user(task_queue.get(True,5))
            except Queue.Empty:
                pass
            except Exception as e:
                logging.exception(e)
                time.sleep(5)
                continue
        control.send('Stop {}'.format(config['id']))
        logging.info('Stop probe_rest id {}'.format(config['id']))

    def probe_stream(self, config, log_queue, control):
        self.probe_log_configurer(log_queue)
        logging.info('Start probe_stream id {}'.format(config['id']))
        try:
            probe = ParserStream(config)
            while True:
                if control.poll():
                    if control.recv() == 'Stop':
                        break
                probe.parse(amount_of_items=200)
        except Exception as e:
            logging.exception(e)
        control.send('Stop {}'.format(config['id']))
        logging.info('Stop probe_stream id {}'.format(config['id']))

    def work(self):
        print('Harvester: started.')
        log_queue = Queue()
        self.probe_log_configurer(log_queue)
        Process(target=self.probe_logger, args=(log_queue,)).start()
        number_of_processes = cpu_count() * 2
        control = list()
        for i in range(number_of_processes):
            parent_conn, child_conn = Pipe()
            control.append((parent_conn, child_conn))
            
        last_time = int(self.config['twitter']['user_in_base']['timeout'])
        list_size = int(self.config['twitter']['user_in_base']['list_limit'])
        task_queue = Queue(maxsize=list_size)

        tmp_config = deepcopy(self.config)
        tmp_config.update({'id':0})
        tmp_config['twitter']['appkeys'] = self.config['twitter']['appkeys']['stream']

        nexus = list()
        nexus.append(Process(target=self.probe_stream,
                args=(tmp_config, log_queue, control[0][1]),
                name='probe_stream'
                ))
        nexus[0].start()
        time.sleep(30)
        for i in range(1,number_of_processes):
            tmp_config['id'] = i
            tmp_config['twitter']['appkeys'] = self.config['twitter']['appkeys']['rest_{}'.format(i)]
            nexus.append(Process(target=self.probe_rest,
                    args=(tmp_config, task_queue, log_queue, control[i][1]),
                    name='probe_rest_{}'.format(i)
                    ))
            nexus[-1].start()

        self.connect_to_database()
        while True:
            users_list = self.db_cursor.get_parse_user_list(
                                        last_time = last_time,
                                        list_size = list_size)
            if users_list:
                for i in users_list:
                    while task_queue.full():
                        time.sleep(5)
                    task_queue.put(i[0])
            else:
                break

        for i in range(number_of_processes):
            control[i][0].send('Stop')
            
        for i in range(1,number_of_processes):
            if not control[i][0].poll():
                nexus[i].join()

        cmd_logger = logging.getLogger('root.cmd_stop')
        cmd_logger.info('Logger: Stop')
        nexus[0].join()
        print('Harvester: stopped.')
                
class DBCursor:
    def __init__ (self, config):
        self.config = config
        try:
            self.db_connection = mysql.connector.connect(
                                            host = self.config['host'],
                                            database = self.config['database_name'],
                                            user = self.config['user_name'],
                                            password = self.config['password'])
        except mysql.connector.Error as e:
            logging.exception("Database connection error. Error code: {}".format(e))
        self.db_cursor = self.db_connection.cursor()
        
        self.add_user = ("INSERT INTO tw_user "
                        "(user_id, username, screen_name, profile_image_url, "
                        "last_tweet_id, last_statuses_count, last_update_time, priority) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
        self.add_tweet = ("INSERT INTO tw_tweet "
                            "(user_id, tweet_id, tweet_date, tweet_text, "
                            "place_longtitude, place_latitude, place_full_name) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        self.update_user = ("UPDATE tw_user SET last_tweet_id = %s, "
                            "last_statuses_count = %s, "
                            "last_update_time = %s, priority = %s "
                            "WHERE user_id = %s")
        self.select_user = ("SELECT last_tweet_id, last_statuses_count "
                            "FROM tw_user WHERE user_id = %s")
        self.select_tweet = "SELECT user_id FROM tw_tweet WHERE tweet_id = %s"
        self.select_users_for_processing = ("SELECT user_id FROM tw_user "
                                            "WHERE last_update_time < %s limit %s")
    
    def add_user_data(self, data):
        try:
            user_data = (data['id'],
                        data['name'],
                        data['screen_name'],
                        data['profile_image_url'],
                        1, 0, 0, 0)
            self.db_cursor.execute(self.add_user, user_data)
            self.db_cursor.execute('COMMIT')
        except Exception as e:
                logging.exception('DBCursor.add_user_data: {} Data: {}'.format(e, user_data))
                return False
        return True
    
    def add_tweet_data(self, data):
        try:
            if data['coordinates'] is None:
                if data['place']['bounding_box'] is None:
                    longtitude = 404
                    latitude = 404
                else:
                    longtitude = (data['place']['bounding_box']['coordinates'][0][0][0] + 
                                    data['place']['bounding_box']['coordinates'][0][2][0])/2
                    latitude = (data['place']['bounding_box']['coordinates'][0][0][1] + 
                                    data['place']['bounding_box']['coordinates'][0][2][1])/2
            else:
                longtitude = data['coordinates']['coordinates'][0]
                latitude = data['coordinates']['coordinates'][1]
            if data['place'] is None:    
                place = 'N/A'
            else:
                place = data['place']['country'] + ', ' + data['place']['full_name']
            tweet_data = (data['user']['id'],
                        data['id'],
                        datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y"),
                        data['text'],
                        longtitude,
                        latitude,
                        place)
            self.db_cursor.execute(self.add_tweet, tweet_data)
            self.db_cursor.execute('COMMIT')
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_DUP_ENTRY:
                return True
            else:
                logging.exception('DBCursor.add_tweet_data: {} Data: {}'.format(e, data))
                return False    
        except Exception as e:
            logging.exception('DBCursor.add_tweet_data: {} Data: {}'.format(e, data))
            return False
        return True
    
    def get_parse_user_list(self, last_time, list_size):
        user_data = (int(time.time())-last_time, list_size)
        self.db_cursor.execute(self.select_users_for_processing, user_data)
        return self.db_cursor.fetchall()
    
    def update_user_statistic(self, data):
        try:
            user_data = (data['last_tweet_id'],
                        data['last_statuses_count'],
                        int(time.time()),
                        0,
                        data['user_id'])
            self.db_cursor.execute(self.update_user, user_data)
            self.db_cursor.execute('COMMIT')
        except Exception as e:
            logging.exception('DBCursor.update_user_statistic: {} Data: {}'.format(e, user_data))
            return False
        return True
        
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
    def __init__ (self, config):
        self.config = config
        self.db_cursor = DBCursor(self.config['database'])
        self.twitter = Twython(self.config['twitter']['appkeys']['app_key'],
                            self.config['twitter']['appkeys']['app_secret'],
                            self.config['twitter']['appkeys']['oauth_token'],
                            self.config['twitter']['appkeys']['oauth_token_secret'])
        
        self.time_start = time.time()
        try:
            data = self.twitter.get_application_rate_limit_status(resources = 'statuses')
            self.timeline_request_counter = (data['resources']['statuses']
                                                ['/statuses/user_timeline']['remaining'])
        except TwythonError as e:
            self.timeline_request_counter = int(self.config['twitter']
                                                    ['user_timeline']['limit_user'])
            logging.exception('ParserREST.__init__: {}'.format(e))

    def parse(self):
        self.process = True
        while self.process:
            users_list = self.db_cursor.get_parse_user_list(
                            last_time = int(self.config['twitter']['user_in_base']['timeout']),
                            list_size = int(self.config['twitter']['user_in_base']['list_limit']))
            if users_list:
                for i in users_list:
                    self.parse_twitter_user(i[0])
            else:
                self.process = False
    
    def parse_twitter_user(self,user_id):
        logging.info('user_id: {}'.format(user_id))
        result = 0
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
                    self.db_cursor.update_user_statistic({
                                'last_tweet_id':result['last_id'],
                                'last_statuses_count':result['statuses_count'],
                                'user_id':user_id})
            else:
                result = self.get_user_timeline(user_id, statistic['last_tweet_id'])
                self.db_cursor.update_user_statistic({
                            'last_tweet_id':result['last_id'],
                            'last_statuses_count':result['statuses_count'] +
                                                statistic['statuses_count'],
                            'user_id':user_id})
        except Exception as e:
            logging.exception('ParserREST.parse_twitter_user: {} Data: {}'.format(e, result))
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
            current_time = time.time()
            if ((current_time - self.time_start) >
                    float(self.config['twitter']['time_window'])):
                self.time_start = current_time
                self.timeline_request_counter = int(self.config['twitter']
                                                    ['user_timeline']['limit_user'])
            if self.timeline_request_counter == 0:
                logging.info('Have to wait.')
                time.sleep(float(self.config['twitter']['time_window']) - (current_time - self.time_start))
            else:
                self.timeline_request_counter = self.timeline_request_counter - 1
                try:
                    if first:
                        user_timeline = self.twitter.get_user_timeline(
                                                user_id = user_id, 
                                                count = int(self.config['twitter']
                                                        ['user_timeline']['page_size']),
                                                since_id = since_id_tmp,
                                                exclude_replies = True,
                                                include_rts = False,
                                                trim_user = True)
                    else:
                        user_timeline = self.twitter.get_user_timeline(
                                                user_id = user_id, 
                                                count = int(self.config['twitter']
                                                        ['user_timeline']['page_size']),
                                                since_id = since_id_tmp,
                                                max_id = max_id,
                                                exclude_replies = True,
                                                include_rts = False,
                                                trim_user = True)
                except TwythonError as e:
                    if e.error_code == 401:
                        process = False
                    logging.exception('ParserREST.get_user_timeline: {}'.format(e))
                    continue
                except ConnectionError as e:
                    logging.exception('ParserREST.get_user_timeline: {}'.format(e))
                    time.sleep(30)
                    self.twitter = Twython(self.config['twitter']['appkeys']['app_key'],
                                        self.config['twitter']['appkeys']['app_secret'],
                                        self.config['twitter']['appkeys']['oauth_token'],
                                        self.config['twitter']['appkeys']['oauth_token_secret'])
                    continue
                if user_timeline:
                    if first:
                        last_id = user_timeline[0]['id']
                        first = False
                    if user_timeline[-1]['id'] == since_id:
                        del(user_timeline[-1])
                        process = False
                    else:
                        max_id = user_timeline[-1]['id'] - 1
                    for status in user_timeline:
                        if (not (status['coordinates'] is None and 
                                        status['place'] is None)):
                            if self.db_cursor.add_tweet_data(status):
                                statuses_count = statuses_count + 1
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
            if ((time.time() - self.time_start) > 
                    float(self.config['twitter']['time_window'])):
                self.time_start = time.time()
                self.timeline_request_counter = int(self.config['twitter']
                                                    ['user_timeline']['limit_user'])
            if self.timeline_request_counter == 0:
                self.timeline_request_counter = int(self.config['twitter']
                                                    ['user_timeline']['limit_user'])
            else:
                self.timeline_request_counter = self.timeline_request_counter - 1
                if first:
                    user_timeline = [{'user':{'id':user_id,
                                            'name':'test_user',
                                            'screen_name':'test_user',
                                            'profile_image_url':'testurl'},
                        'id':since_id + 1,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
                        'text':str(time.time()),
                        'coordinates':{'coordinates':[1, 1]}},
                        {'user':{'id':user_id,'name':'test_user',
                                'screen_name':'test_user',
                                'profile_image_url':'testurl'},
                        'id':since_id,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
                        'text':str(time.time()),
                        'coordinates':{'coordinates':[1, 1]}}]
                    first = False
                else:
                    user_timeline = [{'user':{'id':user_id,
                                    'name':'test_user',
                                    'screen_name':'test_user',
                                    'profile_image_url':'testurl'},
                        'id':since_id + 1,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
                        'text':str(time.time()),
                        'coordinates':{'coordinates':[1, 1]}},
                        {'user':{'id':user_id,'name':'test_user',
                                'screen_name':'test_user',
                                'profile_image_url':'testurl'},
                        'id':since_id,'created_at':'Sun Oct 13 15:07:48 +0000 2013',
                        'text':str(time.time()),
                        'coordinates':{'coordinates':[1, 1]}}]
                if user_timeline:
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
    def __init__ (self, config):
        self.config = config
        TwythonStreamer.__init__(self,
            self.config['twitter']['appkeys']['app_key'],
            self.config['twitter']['appkeys']['app_secret'],
            self.config['twitter']['appkeys']['oauth_token'],
            self.config['twitter']['appkeys']['oauth_token_secret'])
        self.db_cursor = DBCursor(self.config['database'])
        self.username_only = True;
        
    def parse(self, username_only = True, amount_of_items = 1000):
        self.username_only = username_only;
        self.amount_of_items = amount_of_items
        self.process = True
        counter = 0
        last_error_time = time.time()
        while self.process:
            try:
                self.statuses.filter(locations='-180,-90,180,90')
            except Exception as e:
                logging.exception('ParserStream: {}'.format(e))
                if ((time.time() - last_error_time) > 
                            (float(self.config['twitter']['errors']['connection']['timeout']) * 
                            float(self.config['twitter']['errors']['connection']['attempts']))):
                    counter = int(self.config['twitter']['errors']['connection']['attempts'])
                    last_error_time = time.time()
                counter = counter - 1
                if counter == 0:
                    logging.exception("ParserStream: Can not to connect.")
                    self.process = False
                else:
                    time.sleep(int(self.config['twitter']['errors']['connection']['timeout']))

    def test_data(self, start_user_id, tweet_id_step):
        data = {'user':{'id':start_user_id,
                        'name':'test_user',
                        'screen_name':'test_user',
                        'profile_image_url':'testurl'},
                'id':start_user_id + tweet_id_step,
                'created_at':'Sun Oct 13 15:07:48 +0000 2013',
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
                    self.amount_of_items = self.amount_of_items - 1
            else:
                if self.db_cursor.tweet_in_db(data['id']) is None:
                    statistic = self.db_cursor.select_user_statistic(data['user']['id'])
                    if statistic is None:
                        self.db_cursor.add_user_data(data['user'])
                    self.db_cursor.add_tweet_data(data)
                    self.amount_of_items = self.amount_of_items - 1
            if self.amount_of_items == 0:
                self.disconnect()
                self.process = False

    def on_error(self, status_code, data):
        logging.exception('Stream error code: {}'.format(status_code))
        self.disconnect()
        
def main():
    test = Harvester('D:\Dropbox\Programming\Python\config.xml')
    test.work()
        
        
if __name__ == '__main__':
    freeze_support()
    main()
