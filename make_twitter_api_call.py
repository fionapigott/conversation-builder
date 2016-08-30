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
    #logging.debug("Request times list: {}".format(request_times))
    # sleep, if necessary
    if num_requests_in_window >= possible_requests_per_window:
        seconds_to_sleep = (window - (current_time - request_times[-(possible_requests_per_window-1)])).seconds + 5
        logging.debug("To avoid hitting the rate limit, sleeping for {} seconds".format(seconds_to_sleep))
        time.sleep(seconds_to_sleep)
    # get the current time and make a request
    # Update the request times
    current_time = datetime.datetime.now()
    request_times.append(current_time)
    num_requests_in_window = sum([1 for x in request_times if (current_time - x) < window])
    # make the request
    logging.debug("Sending a request to the Twitter Public API for {} Tweets".format(len(tweets_to_query)))
    recovered_tweets_request = requests.post('https://api.twitter.com/1.1/statuses/lookup.json?id=' +
        ",".join(tweets_to_query), 
        auth = auth, headers = {'Content-Type': 'application/json'})
    recovered_tweets = recovered_tweets_request.json()
    # check to be sure we didn't get any errors
    # log errors if we did hit them, wait around if we hit a rate limit
    # debugging: print(ujson.dumps(recovered_tweets))
    if "errors" in recovered_tweets:
        for error in recovered_tweets["errors"]:
            logging.debug("message: {}, code {}".format(error["message"], error["code"]))
            if error["message"] == "Rate limit exceeded":
                # if somehow we did hit the rate limit
                logging.warn("We hit the rate limit (something must be wrong, "+
                    "potentially you had already called the API within the last 15 minutes)"+
                    " but we'll try to get through it.")
                logging.warn("Pausing for {} seconds.".format(window.seconds))
                time.sleep(window.seconds + 5)
                recovered_tweets_request = requests.post('https://api.twitter.com/1.1/statuses/lookup.json?id=' +
                    ",".join(tweets_to_query), 
                    auth = auth, headers = {'Content-Type': 'application/json'})
                current_time = datetime.datetime.now()
                request_times.append(current_time)
        recovered_tweets = recovered_tweets_request.json()
    logging.debug("Completed a call to the Twitter Public API, {} Tweets were returned".format(len(recovered_tweets)))
    # Return the Tweets in a dictionary keyed by Tweet ID
    try:
        recovered_tweets_dict = {x["id_str"]: x for x in recovered_tweets}
    except TypeError:
        logging.error("ERROR Encountered an API error we can't deal with")
        logging.error("ERROR API Response paylaod: {}".format(ujson.dumps(recovered_tweets)))
    return recovered_tweets_dict

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--log', default = 'call_twitter_api.log', help='name of log file')
    parser.add_argument('--credentials', default = '.twurlrc', help='credentials for hitting the Twitter Public API, path from your HOME directory')
    parser.add_argument('--tweet_ids', default = '-', help='file of Tweet IDs, not specified goes to stdin, unquoted Tweet IDs one per line')
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
    tweets_to_query = []
    for line in fileinput.input(args.tweet_ids):
        tweets_to_query.append(line.strip())
        if len(tweets_to_query) == 100:
            recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, 
                window, possible_requests_per_window, auth)
            tweets_to_query = []
            for tweet in recovered_tweets_dict.values():
                print(ujson.dumps(tweet))
    if len(tweets_to_query) > 0:
        recovered_tweets_dict = make_twitter_api_call(tweets_to_query, request_times, 
            window, possible_requests_per_window, auth)
        tweets_to_query = []
        for tweet in recovered_tweets_dict.values():
            print(ujson.dumps(tweet))
