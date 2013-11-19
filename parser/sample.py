test = harvester.Harvester('D:\Dropbox\Programming\Python\config.xml')
test.connect_to_database()

# Using Stream for users' id collection
test.connect_to_twitter_stream()
test.stream.parse()

# Using REST for user's tweets collection
test.connect_to_twitter_rest()
test.rest.parse_twitter_user(1958892991)

# Using REST to collect tweets of users in the database
test.connect_to_twitter_rest()
test.rest.parse()