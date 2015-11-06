# -*- coding: cp1252 -*-

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time, tweepy, json, sys

# Go to http://dev.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key = "pjQGXfqSZSU85dplxlNQ"
consumer_secret = "IQqpj50fNWPFs0XhUlzLkfc9qpqJpgUnCufRXmNp3sw"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token = "1726465267-M6njpV6OYk54Zb6sj8ZG70NhQr1tc43Qgdw2W74"
access_token_secret = "H9cLgchggkfKs57yyW1ZhxfC2MOdzZi1RqXHMYp1Onzlw"

# OAuth process, using the keys and tokens
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    """
    def __init__(self, api = None, fprefix = 'streamer'):
            self.api = api #or API()
            self.counter = 0
            self.tweet_buf = ""
            self.fprefix = fprefix
            self.output  = open(fprefix + '_' + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.delout  = open('delete.txt', 'a')

    def on_data(self, data):

        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return false

    def on_status(self, status):
        # buffer the tweets
        status = status.rstrip('\r\n ')
        self.tweet_buf += status + '\n'
        #print "Tweet #{0}!".format(self.counter + 1)
        # dump at 1000 tweets
        if (self.counter % 100 == 0 and self.counter > 0):
            #print "Logging 1000 tweets..."
            self.output.write(self.tweet_buf)
            del self.tweet_buf
            self.tweet_buf = ""

        self.counter += 1

        if self.counter >= 100000:
            self.output.close()
            self.output = open(self.fprefix + '_' + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.counter = 0

        return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 
    
if __name__ == '__main__':
    l = StdOutListener()
    
    stream = Stream(auth, l, timeout=60.0)
    while True:
        try:
            ## Conterminous USA area
            stream.filter(track=None, locations =[-124.733, 24.55, -95.15,44.816],async=False)
            break

        except Exception, e:
            print '{0} \n...Reconnecting in 60 seconds.'.format(e)
            time.sleep(60)
