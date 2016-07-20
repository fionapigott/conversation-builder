# Author: Fiona Pigott
# Date: July 19, 2016
# Free to use, no guarantees of anything

import pymongo
import fileinput
import ujson
import sys
import argparse
import logging
import field_getters as fg

##################################################################################### Database creation step

def create_database(filename = "-", db_name = "tweet_database"):

    # get the logger
    logging.getLogger("root")

    # this is a pymongo thing
    max_write_value = 1000
    # this could change depending on your RAM
    #max_in_memory_value = args.max_in_memory_value

    # create the database
    client = pymongo.MongoClient()
    tweet_db = client[db_name]
    tweet_collection = tweet_db["tweet_collection"]
    #tweet_collection.drop()
    _ = tweet_collection.create_index([('tweet_id', pymongo.ASCENDING)],unique=True)
    _ = tweet_collection.create_index([('reply_id', pymongo.ASCENDING)],unique=False)
    if tweet_collection.count() == 0:
        logging.debug('Created a database and collection in MongoDB. No Tweets have been added yet.')
    else:
        logging.warn('WARNING: There are already records in this collection. ' + 
            'This could have been caused by a previous script exiting before cleaning up.')
        logging.warn('WARNING: Collection size is: {}'.format(tweet_collection.count()))

    # store the records that we will insert
    records = tweet_collection.initialize_unordered_bulk_op()
    # count all of the tweet ids and the duplicate ids
    duplicate_tweet_ids = []
    tweet_ids = set()
    wrote_something = False
    num_records = 0
    log_at = 0
    log_val = 10
    for line in fileinput.input(filename):
        # get a valid tweet
        try:
            tweet = ujson.loads(line)
            tweet_id = fg.tweet_id(tweet) 
        except (ValueError, KeyError):
            continue
        # if this tweet id is in the set of tweet ids we already have, ignore it. 
        # tweet ids should be unique
        if tweet_id in tweet_ids:
            continue
        else:
            tweet_ids |= {tweet_id}
        # now try getting the reply field
        reply_info = fg.reply_info(tweet)
        # now add the tweet to the current list of records
        # we can't hold all of the Tweets in memory at a time, so we'll do 10k at a time for now
        records.insert({"tweet_id": tweet_id,
                        "user_id": fg.user_id(tweet), 
                        "in_reply_to_id": reply_info["reply_id"],
                        "in_reply_to_user": reply_info["reply_user"],
                        "in_reply_to_user_id": reply_info["reply_user_id"],
                        "tweet_payload": line})
        num_records += 1
        # once we have x records, insert the Tweets into the MongoDb database
        if num_records >= max_write_value:
            try:
                records.execute()
                wrote_something = True
                if log_at >= log_val:
                    logging.debug('Collection contains {} Tweets. Still writing.'.format(tweet_collection.count()))
                    log_at = 0
                else:
                    log_at += 1
            # if we still have a duplicate tweet id running around catch it
            except pymongo.errors.BulkWriteError as bwe:
                pass
                #duplicate_tweet_ids.append(bwe.details["writeErrors"][0]["op"]["tweet_id"]) # debugging
            # reset the records that we are going to insert    
            records = tweet_collection.initialize_unordered_bulk_op()
            num_records = 0


    # at the end of the loop, insert the remainder of the records
    try:
        records.execute()
        wrote_something = True
        logging.debug('Collection contains {} Tweets. Still writing.'.format(tweet_collection.count()))
    # if we still have a duplicate tweet id running around catch it
    except pymongo.errors.BulkWriteError as bwe:
        #duplicate_tweet_ids.append(bwe.details["writeErrors"][0]["op"]["tweet_id"]) # debugging
        logging.debug('Collection contains {} Tweets. Still writing.'.format(tweet_collection.count()))
    except pymongo.errors.InvalidOperation:
        if not wrote_something:
            logging.warn('No Tweets were written to the collection. ' + 
                'Any Tweets in the collection are left over from some preious process.')
            logging.warn('Collection contains {} Tweets.'.format(tweet_collection.count()))
        pass

    logging.debug('Collection contains {} Tweets. Done writing'.format(tweet_collection.count()))

    del(records)

    return client, "tweet_database", tweet_collection
