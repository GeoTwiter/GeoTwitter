import harvester

test = Harvester('D:\Dropbox\Programming\Python\config.xml')
test.connect_to_database()
test.connect_to_twitter_stream()
test.connect_to_twitter_rest()

test.stream.parse()
test.stream.statuses.filter(locations='-180,-90,180,90')
test.stream.statuses.filter(track='twitter')

test.stream.test_data()

test.rest.parse_twitter_user(1958892991)