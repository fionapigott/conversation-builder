# Author: Fiona Pigott
# Date: July 20, 2016
# Free to use, no guarantees of anything

import ujson
import sys
import argparse
import logging
import fileinput
from get_brand_info import get_brand_info
from snowflake2utc import snowflake2utc
import enrichment_functions as enrich


def add_enrichments(conversation_payload, brands = []):
    '''
    Add relevant metadata to the conversation.
    Preserving the fields that are already in the data, add relevant metadata (enrichments)

    The fields that a conversation will always come in with are:
    {
        "tweets": [  # time-sorted list of Tweets
            { < Tweet payload > }, # if the first Tweet was missing, it has the format: {"missing_tweet_id": _, "screen_name": _, "user_id": _}
            { < Tweet payload > }  
          ],  
        "depths": [0,1...] #List of depths, same order as the tweets list  
    }
    '''
    # the metadata step calculates some extra information about Tweets. 
    # These fields can easily be modified/added to
    # now calculate some relevant metadata about the conversation
    # this is relevant whether or not we have "brands" we care about
    enrichments = {
     "size_of_conversation": enrich.size_of_conversation(conversation_payload), 
     "approx_depth": enrich.approx_depth(conversation_payload),
     "root_user": enrich.root_user(conversation_payload),
     "time_to_first_response": enrich.time_to_first_response(conversation_payload),
     "duration_of_conversation": enrich.duration_of_conversation(conversation_payload),
     "ids_of_missing_tweets": enrich.ids_of_missing_tweets(conversation_payload),
    }
    conversation_payload.update(enrichments)
    return conversation_payload

def add_brand_enrichments(conversation_payload, brands):
    '''
    Add relevant metadata to the conversation.
    Preserving the fields that are already in the data, add relevant metadata (enrichments)

    The fields that a conversation will always come in with are:
    {
        "tweets": [  # time-sorted list of Tweets
            { < Tweet payload > }, 
            { < Tweet payload > }  
          ],  
        "depths": [0,1...] #List of depths, same order as the tweets list  
    }

    This function expects a "brands" input, 
    which is a list of dictionaries of brands' ids and screen names:
    [ {"screen_name": brand1, "user_id": 11111}, {"screen_name": brand2, "user_id": 22222}, ... ]
    '''
    # this is only relevant if brands were provided
    brands_enrichments = {
     "time_to_first_brand_response": enrich.time_to_first_brand_response(conversation_payload, brands),
     "first_brand_response": enrich.first_brand_response(conversation_payload, brands),
     "brands_tweeting": enrich.brands_tweeting(conversation_payload, brands),
     "nonbrands_tweeting": enrich.nonbrands_tweeting(conversation_payload, brands),
     "brands_mentioned": enrich.brands_mentioned(conversation_payload, brands),
     "nonbrands_mentioned": enrich.nonbrands_mentioned(conversation_payload, brands),
    }
    conversation_payload.update(brands_enrichments)

    return conversation_payload

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--log', default = 'add_enrichments.log', help='name of log file')
    parser.add_argument('--brand_info', default = None)
    parser.add_argument('--input', default = '-', help='name of input Tweet data, default is stdin')
    args = parser.parse_args()

    logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: In file: %(name)s, On line %(lineno)d: %(message)s')
    logging.debug('###################################################################### ' + 
        'adding enrichments to conversations from '.format(args.input))

    # add brand enrichments
    if args.brand_info is not None:
        do_brand_enrichments = True
        brands = get_brand_info(args.brand_info) 
    else:
        do_brand_enrichments = False

    for line in fileinput.input(args.input):
        # deserialize the JSON
        try:
            conversation_payload = ujson.loads(line)
        except ValueError:
            continue
        if ("tweets" in conversation_payload) and ("depths" in conversation_payload):
            # add enrichments
            conversation_payload = add_enrichments(conversation_payload)
            if do_brand_enrichments:
                conversation_payload = add_brand_enrichments(conversation_payload, brands)
            print(ujson.dumps(conversation_payload))
        else:
            logging.warn("WARNING: Found a conversation that is not a valid conversation payload on line {}".format(fileinput.lineno()))
            logging.warn("WARNING: Skipping line {}".format(fileinput.lineno()))        



