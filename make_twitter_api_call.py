# Author: Fiona Pigott
# Date: July 20, 2016
# Free to use, no guarantees of anything

import sys
import logging
import datetime
from requests_oauthlib import OAuth1
import requests
import time

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
