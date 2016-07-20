# Author: Fiona Pigott
# Date: July 19, 2016
# Free to use, no guarantees of anything

import fileinput
import ujson
import sys
import argparse
import logging
#import field_getters as fg
from get_brand_info import get_brand_info
#import add_metadata
from get_authentication import get_authentication
from make_twitter_api_call import make_twitter_api_call
from hydrate_recovered_tweets import hydrate_recovered_tweets
import datetime


parser = argparse.ArgumentParser()
parser.add_argument('--brand_info', default = None, help='csv of brand screen name and brand id (e.g.: "notFromShrek,5555555")')
parser.add_argument('--log', default = 'add_missing_tweets.log', help='name of log file')
parser.add_argument('--credentials', default = '.twurlrc', help='credentials for hitting the Twitter Public API, path from your HOME directory')
args = parser.parse_args()

logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: In file: %(name)s, On line %(lineno)d: %(message)s')
logging.debug('###################################################################### ' + 
    'adding missing Tweets to a set of conversations')

##################################################################################### Get brand information step

brands = get_brand_info(args.brand_info)

##################################################################################### Setup your credentials to call the Twitter Public API

auth = get_authentication(args.credentials)

##################################################################################### Adding the new Tweets to the conversation payloads

# Keep track of the Tweets we need to query for
tweets_to_query = []
convos_in_memory = []

# Kepp track of when queries have been made so that we don't go over the request limit
window = datetime.timedelta(minutes = 15)
possible_requests_per_window = 180
request_times = [datetime.datetime.now() - datetime.timedelta(days = 1)]

# Read in the conversations
for line in fileinput.input("-"):
    # read in a conversation payload
    try:
        convo_dict = ujson.loads(line)
    except ValueError:
        continue
    # get the missing Tweets
    missing_tweets = [str(x) for x in convo_dict["ids_of_missing_tweets"]]
    # If there are no missing Tweets, pass this conversation through and move on
    if len(missing_tweets) == 0:
        print(line.strip())
    # If we don't yet have 100 Tweets to query, add these to the total and move on
    elif len(missing_tweets) + len(tweets_to_query) < 100:
        tweets_to_query.extend(missing_tweets)
        convos_in_memory.append(convo_dict)
    # If we have 100 Tweets to query, add some missing Tweets
    else:
        recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
        # add the missing Tweets
        hydrate_recovered_tweets(convos_in_memory, recovered_tweets_dict, brands)
        tweets_to_query = []
        convos_in_memory = []

if len(tweets_to_query) > 0:
    recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
    # add the missing Tweets
    hydrate_recovered_tweets(convos_in_memory, recovered_tweets_dict, brands)












