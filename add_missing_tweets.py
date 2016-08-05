# Author: Fiona Pigott
# Date: July 19, 2016
# Free to use, no guarantees of anything

import fileinput
import ujson
import sys
import os
import argparse
import logging
import datetime
import time
import yaml
from requests_oauthlib import OAuth1
import requests
import field_getters as fg
from get_brand_info import get_brand_info
import add_enrichments
from snowflake2utc import snowflake2utc


def get_authentication(credentials_file):
    '''
    access the creds file, "credentials_file" is a path from your HOME directory
    I'm going to use the default .twurlrc setup for a credentials file. 
    If you already have a .twurlrc, it should work. Otherwise, create one. It should look like this:
    --- 
    configuration: 
      default_profile: 
      - notFromShrek
      - < CONSUMER KEY >
    profiles: 
      notFromShrek: 
        < CONSUMER KEY >: 
          username: notFromShrek
          token: < TOKEN >
          secret: < SECRET >
          consumer_secret: < CONSUMER SECRET >
          consumer_key: < CONSUMER KEY >
    '''

    # get the logger
    logging.getLogger("root")

    creds = yaml.load(open(os.getenv('HOME') + "/" + credentials_file ,"r")) 
    keys = creds["profiles"][creds["configuration"]["default_profile"][0]][creds["configuration"]["default_profile"][1]]
    auth = OAuth1(keys["consumer_key"],keys["consumer_secret"],keys["token"],keys["secret"]) 
    
    return auth

def make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth):
    '''
    Wait the appropriate amount of time, then make a request to the Twitter Public API for 'tweets_to_query'
    Arguments:
        - tweets_to_query: list of strings of Tweet ids
        - request_times: list of datetime.datetime objects that contains the history of all request times up until now
        - window: datetime.timedelta object that says how long a rate limit window is 
        - possible_requests_per_window: how many requests we can make per window 
        - auth: OAuth1 authentication object 
    Returns:
        - upadates in place the list of the history of requests
        - a dictionary of Tweets returned from the call
    '''
    # get the logger
    logging.getLogger("root")
    # If "tweets_to_query" is an empty list, don't do anything
    if len(tweets_to_query) == 0:
        return {}
    # Get the current time (for the rate limit)
    current_time = datetime.datetime.now()
    # get the number of requests in the current window
    num_requests_in_window = sum([1 for x in request_times if (current_time - x) < window]) 
    # sleep, if necessary
    if num_requests_in_window > possible_requests_per_window:
        seconds_to_sleep = (window - (current_time - request_times[-possible_requests_per_window])).seconds + 5
        logging.debug("To avoid hitting the rate limit, sleeping for {} seconds".format(seconds_to_sleep))
        time.sleep(seconds_to_sleep)
    # get the current time and make a request
    # Update the request times
    current_time = datetime.datetime.now()
    request_times.append(current_time)
    num_requests_in_window = sum([1 for x in request_times if (current_time - x) < window])
    # make the request
    logging.debug("Sending a request to the Twitter Public API for {} Tweets".format(len(tweets_to_query)))
    recovered_tweets_request = requests.post('https://api.twitter.com/1.1/statuses/lookup.json?id=' + ",".join(tweets_to_query), 
        auth = auth, headers = {'Content-Type': 'application/json'})
    recovered_tweets = recovered_tweets_request.json()
    logging.debug("Completed a call to the Twitter Public API, {} Tweets were returned".format(len(recovered_tweets)))
    # Return the Tweets in a dictionary keyed by Tweet ID
    recovered_tweets_dict = {x["id_str"]: x for x in recovered_tweets}
    return recovered_tweets_dict

def collect_missing_tweets(filename = "-", max_convos_in_memory = 1000, tweets_per_call = 100):#, request_times = [], credentials = '.twurlrc'):
    '''
    Iterator to batch missing Tweets into sets of 100 to make efficient API calls.

    Takes a file (or simple stdin, which is default) of conversation payloads, parses out the missing_tweet_id(s) 
    and yields a list of Tweets to query (up to 100 Tweets) and a list of conversations from which those Tweets are missing.

    These lists can then be used to call the Twitter API and insert the missing Tweets into the conversations.

    A conversation with no missing Tweets will be returned with an empty list of "Tweets to query"
    '''
    # get the logger
    logging.getLogger("root")
    # read in the data
    tweets_to_query = []
    convos_in_memory = []
    for line in fileinput.input(filename):
        # deserialize
        try:
            conversation_payload = ujson.loads(line)
        except ValueError:
            logging.warn("Found a bad JSON payload on line {}".format(fileinput.lineno()))
            continue
        # get the missing Tweets
        missing_tweets = [x["missing_tweet_id"] for x in conversation_payload["tweets"] if "missing_tweet_id" in x]
        # If there are no missing Tweets, pass this conversation through and move on
        if len(missing_tweets) == 0:
            #convos_in_memory.append(conversation_payload)
            yield(([],[conversation_payload]))
        # If we don't yet have 100 Tweets to query, or there aren't too many convos in memory
        # add these to the total and move on
        elif (len(missing_tweets) + len(tweets_to_query) < tweets_per_call) and (len(convos_in_memory) < max_convos_in_memory):
            tweets_to_query.extend(missing_tweets)
            convos_in_memory.append(conversation_payload)
        else:
            yield((tweets_to_query, convos_in_memory))
            tweets_to_query = []
            conversations_in_memory = []
    # once you get to the end and have a remainder
    if len(convos_in_memory) > 0:
        yield((tweets_to_query, convos_in_memory)) 

        
def insert_missing_tweets(conversations_in_memory, recovered_tweets_dict):
    '''
    Take a list of conversations, and a dictionary (keyed by Tweet ID) of the Tweets that were missing from those conversations.
    Insert the recovered tweets into the conversation and return a conversation payload dictionary, with the fields: 

    {
        "tweets": [  # time-sorted list of Tweets
            { < Tweet payload > }, # if the first Tweet was missing, it is an empty dictionary in the payload  
            { < Tweet payload > }  
          ],  
        "depths": [0,1...] #List of depths, same order as the tweets list. 
        "ids_of_missing_tweets": # List of Tweets ids of missing Tweets
        "recovered_tweets": #List of Tweets that were missing but are now recovered
        "new_missing_tweets": #List of Tweets that is discovered missing when a previously missing Tweet was recovered (!)
        "unrecoverable_tweets": The Twitter API did not return these Tweets (probably deleted)
    }

    (!) A Tweet that we have discovered is missing (when a missing Tweet was actually a reply to a different Tweet) 
    is *not* recovered, and its depth is  = {depth of Tweet that replied to it} - 1

    If a conversation has no missing Tweets at all, add empty lists for all of these extra fields
    '''
    # hydrate the conversation
    # for each conversation that we currently have in memory (up to 100)
    for conversation_payload in conversations_in_memory:
        # fields that may need updating
        tweets = []
        ids_to_depths_dict = dict(zip([fg.tweet_id(x) for x in conversation_payload["tweets"]], conversation_payload["depths"]))
        new_missing_tweets = []
        recovered_tweets_ids = []
        unrecoverable_tweets = []
        # now we have to re-create the hydrated_conversation list
        for i,tweet in enumerate(conversation_payload["tweets"]):
            # if the Tweet was never missing
            if "missing_tweet_id" not in tweet:
                tweets.append(tweet)
            # if the Tweet was missing
            else:
                # see if it was returned by the request (if not, it may have been deleted)
                try:
                    recovered_tweet = recovered_tweets_dict[tweet["missing_tweet_id"]]
                    tweets.append(recovered_tweet)
                    recovered_tweets_ids.append(fg.tweet_id(recovered_tweet))
                    # if it was a reply we have another "missing" Tweet
                    recovered_tweet_reply_info = fg.reply_info(recovered_tweet)
                    if recovered_tweet_reply_info["reply_id"] != "NOT_A_REPLY":
                        tweets.append(
                           {"missing_tweet_id": recovered_tweet_reply_info["reply_id"],
                            "screen_name": recovered_tweet_reply_info["reply_user"],
                            "user_id": recovered_tweet_reply_info["reply_user_id"]}
                        )
                        # add an item ("original tweet depth -1") to the depths list
                        new_depth = ids_to_depths_dict[tweet["missing_tweet_id"]] - 1
                        ids_to_depths_dict.update({recovered_tweet_reply_info["reply_id"]: new_depth})
                        new_missing_tweets.append(recovered_tweet_reply_info["reply_id"])
                # or just return the missing Tweet placeholder
                except KeyError:
                    tweets.append(tweet)
                    unrecoverable_tweets.append(tweet["missing_tweet_id"])
        # update the conversation
        # add a little information about the Tweets that were recovered
        sorted_tweets = sorted(tweets, key = lambda x: snowflake2utc(fg.tweet_id(x)))
        sorted_depths = [ids_to_depths_dict[fg.tweet_id(x)] for x in sorted_tweets] 
        conversation_payload.update({
            "tweets": sorted_tweets,
            "depths": sorted_depths,
            "ids_of_missing_tweets": list(set(new_missing_tweets) | set(unrecoverable_tweets)),
            "recovered_tweets": recovered_tweets_ids, 
            "new_missing_tweets": new_missing_tweets, 
            "unrecoverable_tweets": unrecoverable_tweets})
        # print the conversation payload
        yield(conversation_payload)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--log', default = 'add_missing_tweets.log', help='name of log file')
    parser.add_argument('--credentials', default = '.twurlrc', help='credentials for hitting the Twitter Public API, path from your HOME directory')
    parser.add_argument('--raw_data_only', action='store_true', help='dont expect conversation payloads, just expect Tweet IDs and return row-level Tweet data')
    parser.add_argument('--tweet_ids', default = '-', help='file of Tweet IDs, not specified goes to stdin, unquoted Tweet IDs one per line')
    parser.add_argument('--add_enrichments', action='store_true', help='add (or update) enrichment fields to these conversations')
    parser.add_argument('--brand_info', default = None, help='csv of brand screen name and brand id (e.g.: "notFromShrek,5555555"), used if you are updating enrichements')
    args = parser.parse_args()

    logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: In file: %(name)s, On line %(lineno)d: %(message)s')
    logging.debug('###################################################################### ' + 
        'adding missing Tweets to a set of conversations')

    # get your credentials 
    auth = get_authentication(args.credentials)
    # Keep track of when queries have been made so that we don't go over the request limit
    # Twitter API limits are hardcoded here: 15 minute window, 180 requests per window
    window = datetime.timedelta(minutes = 15)
    possible_requests_per_window = 180
    request_times = [datetime.datetime.now() - datetime.timedelta(days = 1)]

    # get raw data
    if args.raw_data_only:
        tweets_to_query = []
        for line in fileinput.input(args.tweet_ids):
            if len(tweets_to_query) < 100:
                tweets_to_query.append(line)
            else:
                recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
                tweets_to_query = []
                for tweet in recovered_tweets_dict.values():
                    print(ujson.dumps(tweet))
        if len(tweets_to_query) > 0:
            recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
            tweets_to_query = []
            for tweet in recovered_tweets_dict.values():
                print(ujson.dumps(tweet))

    # add missing Tweets to conversation payloads
    else:
        # get brand info if you need it
        if args.add_enrichments:
            if args.brand_info is not None:
                do_brand_enrichments = True
                brands = get_brand_info(args.brand_info)
            else:
                do_brand_enrichments = False

        for tweets_to_query,convos in collect_missing_tweets(filename = "-", max_convos_in_memory = 10000,tweets_per_call = 100):
            # get the Tweets
            recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, window, possible_requests_per_window, auth)
            # insert the Tweets into the conversations
            for conversation_payload in insert_missing_tweets(convos, recovered_tweets_dict):
                # optionally update enrichments in the conversation payload
                if args.add_enrichments:
                    # add enrichments
                    conversation_payload = add_enrichments.add_enrichments(conversation_payload)
                    if do_brand_enrichments:
                        conversation_payload = add_enrichments.add_brand_enrichments(conversation_payload, brands)
                print(ujson.dumps(conversation_payload))









