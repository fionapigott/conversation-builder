# Author: Fiona Pigott
# Date: July 20, 2016
# Free to use, no guarantees of anything

from snowflake2utc import snowflake2utc
from hydration_functions import *

def add_metadata(unordered_hydrated_conversation, brands = []):
    '''
    Add relevant metadata to the conversation.
    The output of this is what is ultimately returned as a "conversation" payload

    A unordered_hydrated_conversation is a list of Tweets payloads and relevant data, with exactly these keys and data:
    [ {"screen_name": < screen name of user tweeting >, 
       "user_id": < user id of user Tweeting >,
       "missing": < whether or not this Tweet is missing >,
       "depth": < how far down in the tree the Tweet is >,
       "in_reply_to": < id of Tweet being replied to >,
       "id": < integer number id of Tweet, I use the snowflake ID to get time, so this ID cannot be replaced by any other unique identifier >,
       "tweet": < dictionary payload of the Tweet data (decoded JSON payload) >},
       { < more Tweets, same format > },
       ...
       ...
    ]

    Some of these functions expect a "brands" input, which is a list of dictionaries of brands' ids and screen names:
    [ {"screen_name": brand1, "user_id": 11111}, {"screen_name": brand2, "user_id": 22222}, ... ]
    '''

    # make sure that the hydrated conversation is time-sorted
    hydrated_conversation = sorted(unordered_hydrated_conversation, key = lambda x: snowflake2utc(x["id"]))
    # the metadata step calculates some extra information about Tweets. 
    # These fields can easily be modified/added to
    # now calculate some relevant metadata about the conversation
    conversation_with_metadata = {
     "tweets": [x["tweet"] for x in hydrated_conversation]
    }
    # this is relevant whether or not we have "brands" we care about
    metadata = {
     "size_of_conversation": size_of_conversation(hydrated_conversation), 
     "approx_depth": approx_depth(hydrated_conversation),
     "root_user": root_user(hydrated_conversation),
     "time_to_first_response": time_to_first_response(hydrated_conversation),
     "duration_of_conversation": duration_of_conversation(hydrated_conversation),
     "ids_of_missing_tweets": ids_of_missing_tweets(hydrated_conversation),
     "depths": depths(hydrated_conversation)
    }
    conversation_with_metadata.update(metadata)
    # this is only relevant if brands were provided
    if len(brands) > 0:
        brands_metadata = {
         "time_to_first_brand_response": time_to_first_brand_response(hydrated_conversation, brands),
         "first_brand_response": first_brand_response(hydrated_conversation, brands),
         "brands_tweeting": brands_tweeting(hydrated_conversation, brands),
         "nonbrands_tweeting": nonbrands_tweeting(hydrated_conversation, brands),
         "brands_mentioned": brands_mentioned(hydrated_conversation, brands),
         "nonbrands_mentioned": nonbrands_mentioned(hydrated_conversation, brands),
        }
        conversation_with_metadata.update(brands_metadata)

    return conversation_with_metadata