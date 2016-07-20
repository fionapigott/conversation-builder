# Author: Fiona Pigott
# Date: July 20, 2016
# Free to use, no guarantees of anything

import sys
import logging
import datetime
from requests_oauthlib import OAuth1
import requests
import time
import fileinput
import ujson
import sys
import argparse
import logging
from get_authentication import get_authentication
from make_twitter_api_call import make_twitter_api_call
import datetime


parser = argparse.ArgumentParser()

parser.add_argument('--log', default = 'public_api_requests.log', help='name of log file')
parser.add_argument('--credentials', default = '.twurlrc', help='credentials for hitting the Twitter Public API, path from your HOME directory')
args = parser.parse_args()

logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: In file: %(name)s, On line %(lineno)d: %(message)s')
logging.debug('###################################################################### ' + 
    'adding missing Tweets to a set of conversations')

##################################################################################### Setup your credentials to call the Twitter Public API

auth = get_authentication(args.credentials)

##################################################################################### Graph creation step

# Keep track of the Tweets we need to query for
tweets_to_query = []
convos_in_memory = []

# Kepp track of when queries have been made so that we don't go over the request limit
window = datetime.timedelta(minutes = 15)
possible_requests_per_window = 180
request_times = [datetime.datetime.now() - datetime.timedelta(days = 1)]

# Read in the Tweet IDS
for line in fileinput.input("-"):
    # Tweet ID
    tweets_to_query.append(line.strip())
    # If we don't yet have 100 Tweets to query, add these to the total and move on
    # If we do have 100 Tweets: 
    if len(tweets_to_query) == 100:
        recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
        for tweet_id in recovered_tweets_dict:
            ujson.dumps(recovered_tweets_dict[tweet_id])
        tweets_to_query = []

if len(tweets_to_query) > 0:
    recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
    for tweet_id in recovered_tweets_dict:
        ujson.dumps(recovered_tweets_dict[tweet_id])
    tweets_to_query = []