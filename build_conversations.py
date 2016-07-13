# Authoer: Fiona Pigott
# Date: July 13, 2016
# Free to use, no guarantees of anything

import pymongo
import fileinput
import ujson
from find_children import find_children
from hydration_functions import *
from snowflake2utc import snowflake2utc
import sys
import argparse
import logging
import field_getters as fg

##################################################################################### Parse args and set up logging

parser = argparse.ArgumentParser()
parser.add_argument('--brand_info', default = 'no brands', help='csv of brand screen name and brand id (e.g.: "notFromShrek,5555555")')
parser.add_argument('--format', default = 'activity-streams', help='choose "activity-streams" or "original", default "activity-streams"')
parser.add_argument('--max_in_memory_value', type = int, default = 10000, 
    help='maximum number of Tweets to hold in memory at a single time, default 10k')
parser.add_argument('--log', default = 'build_conversations.log', help='name of log file')
args = parser.parse_args()

logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: On line %(lineno)d: %(message)s')
logging.debug('###################################################################### ' + 
    'building a new set of conversations')

##################################################################################### The format (activity-streams or original format)

if args.format == "activity-streams":
  format = True
  logging.debug('Using the gnip activity-streams data format.')
else:
  format = False
  logging.debug('Using the original (public Twitter API) data format.')

##################################################################################### Get brand information step

brands = []
if args.brand_info != 'no brands':
  for line in fileinput.input(args.brand_info):
      info = line.split(",")
      brands.append({"screen_name": info[0].strip().lower(), "user_id": info[1].strip()})

logging.debug('The brands are: {}'.format([x["screen_name"] for x in brands]))

##################################################################################### Database creation step

# this is a pymongo thing
max_write_value = 1000
# this could change depending on your RAM
max_in_memory_value = args.max_in_memory_value

# create the database
client = pymongo.MongoClient()
tweet_db = client.tweet_database
tweet_collection = tweet_db.tweet_collection
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
for line in fileinput.input("-"):
    # get a valid tweet
    try:
        tweet = ujson.loads(line)
        tweet_id = fg.tweet_id(tweet, format) #tweet["id"].split(":")[-1]
    except (ValueError, KeyError):
        continue
    # if this tweet id is in the set of tweet ids we already have, ignore it. 
    # tweet ids should be unique
    if tweet_id in tweet_ids:
        continue
    else:
        tweet_ids |= {tweet_id}
    # now try getting the reply field
    reply_info = fg.reply_info(tweet, format)
    # now add the tweet to the current list of records
    # we can't hold all of the Tweets in memory at a time, so we'll do 10k at a time for now
    records.insert({"tweet_id": tweet_id,
                    "user_id": fg.user_id(tweet, format), 
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
            duplicate_tweet_ids.append(bwe.details["writeErrors"][0]["op"]["tweet_id"]) # debugging
        # reset the records that we are going to insert    
        records = tweet_collection.initialize_unordered_bulk_op()
        num_records = 0


# at the end of the loop, insert the remainder of the records
try:
    records.execute()
    wrote_something = True
    
# if we still have a duplicate tweet id running around catch it
except pymongo.errors.BulkWriteError as bwe:
    duplicate_tweet_ids.append(bwe.details["writeErrors"][0]["op"]["tweet_id"]) # debugging
    logging.debug('Collection contains {} Tweets. Still writing.'.format(tweet_collection.count()))
except pymongo.errors.InvalidOperation:
    if not wrote_something:
        logging.warn('No Tweets were written to the collection. ' + 
            'Any Tweets in the collection are left over from some preious process.')
        logging.warn('Collection contains {} Tweets.'.format(tweet_collection.count()))
    pass

logging.debug('Collection contains {} Tweets. Done writing'.format(tweet_collection.count()))

del(records)

##################################################################################### Graph creation step

# get links from parent to child nodes
parent_to_children = {
    x["_id"]: {"children": x["children"], "in_reply_to_user": x["in_reply_to_user"], "in_reply_to_user_id": x["in_reply_to_user_id"] } 
      for x in tweet_collection.aggregate([
            {"$group": { "_id": "$in_reply_to_id", 
                         "children": {"$push" : "$tweet_id"},
                         "in_reply_to_user": {"$first" : "$in_reply_to_user"},
                         "in_reply_to_user_id": {"$first" : "$in_reply_to_user_id"}
                       }}])
    }

logging.debug('There were {} individual Tweets in the input.'.format(tweet_collection.count()))

# make sure we have a "NOT_A_REPLY" key
if "NOT_A_REPLY" not in parent_to_children:
    parent_to_children["NOT_A_REPLY"] = {"children": [], "in_reply_to_user": "NOT_A_REPLY"}

# get the root nodes so that we can build the graphs
root_nodes = []
all_children = []
for key in parent_to_children:
    all_children.extend(parent_to_children[key]["children"])

logging.debug('There are {} Tweets involved in the conversation'.format(len(set(all_children) | set(parent_to_children.keys()) - set(["NOT_A_REPLY"]))) + 
   ' (some Tweets appear in an "inReplyTo" field, so we know they existed, but they were not in the dataset)')

# keys that are not children + tweets that are not replies
root_nodes = (set(parent_to_children.keys()) - set(["NOT_A_REPLY"]) - set(all_children)) | (set(parent_to_children["NOT_A_REPLY"]["children"])) 
logging.debug('There are {} individual conversation threads.'.format(len(root_nodes)))
del(all_children)

# all of the conversation graphs
multi_node_graphs = []
# group the tweets together in conversations
for root in root_nodes:
    children = find_children(root, None, 0, parent_to_children)
    multi_node_graphs.append(sorted(children, key=lambda k: k["depth"]))

# in case of missing tweets, we want some info about the originating user
tweet_to_screenname = {k: {"user_id": v["in_reply_to_user_id"], "screen_name": v["in_reply_to_user"]} for k,v in parent_to_children.items()}

del(parent_to_children)
del(root_nodes)

logging.debug('Finished buiding the tree graph structure.')

##################################################################################### Graph hydration step

# break up the graphs into pieces so that we can query for each piece
shards = {0: {"tweets":[], "start": 0, "end": 1}}
shard_number = 0
for i,graph in enumerate(multi_node_graphs):
    if len(shards[shard_number]["tweets"]) + len(graph) > max_in_memory_value:
        shard_number += 1
        shards[shard_number] = {"tweets":[], "start": i, "end": i + 1}
    shards[shard_number]["tweets"].extend([x["tweet_id"] for x in graph])
    shards[shard_number]["end"] = i + 1

shard_number += 1

logging.debug('Broke the data into shards. There are {} shards '.format(shard_number) + 
    '(this is the number of calls that will be made to the database)')

##################################################################################### 

logging.debug('Beginning to hydrate conversations.')
#hydrated_conversations = [] #debugging
for item,shard in list(shards.items()):
    # load up a shard of conversations
    id_to_tweet = {x["tweet_id"]: ujson.loads(x["tweet_payload"])
                     for x in tweet_collection.find( { "tweet_id": {"$in": shard["tweets"]} } )}
    # grab the conversations that we care about
    for conversation in multi_node_graphs[shard["start"]:shard["end"]]:
        # the "hydration" step provides a list of Tweets and some data about them
        # now "hydrate" each conversation (give it the actual tweet)
        hydrated_conversation = []
        for tweet in conversation:
            try:
                # if it is a Tweet in our dataset
                tweet_dict = id_to_tweet[tweet["tweet_id"]]
                hydrated_conversation.append({"screen_name": fg.screen_name(tweet_dict, format), 
                                              "user_id": fg.user_id(tweet_dict, format), 
                                              "missing": False,
                                              "depth": tweet["depth"],
                                              "in_reply_to": tweet["in_reply_to"],
                                              "id": int(tweet["tweet_id"]), 
                                              "tweet": tweet_dict})
            except KeyError:
                # if it's not a Tweet in our dataset
                hydrated_conversation.append({"screen_name": tweet_to_screenname[tweet["tweet_id"]]["screen_name"],
                                              "user_id": tweet_to_screenname[tweet["tweet_id"]]["user_id"],
                                              "missing": True,
                                              "depth": tweet["depth"],
                                              "id": int(tweet["tweet_id"]),
                                              "tweet": {}})
        # make sure that the hydrated conversation is always time-sorted
        hydrated_conversation.sort(key = lambda x: snowflake2utc(x["id"]))
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
         "ids_of_missing_tweets": ids_of_missing_tweets(hydrated_conversation)
        }
        conversation_with_metadata.update(metadata)
        # this is only relevant if brands were provided
        if len(brands) > 0:
            brands_metadata = {
             "time_to_first_brand_response": time_to_first_brand_response(hydrated_conversation, brands),
             "first_brand_response": first_brand_response(hydrated_conversation, brands),
             "brands_tweeting": brands_tweeting(hydrated_conversation, brands),
             "nonbrands_tweeting": nonbrands_tweeting(hydrated_conversation, brands),
             "brands_mentioned": brands_mentioned(hydrated_conversation, brands, format),
             "nonbrands_mentioned": nonbrands_mentioned(hydrated_conversation, brands, format),
            }
            conversation_with_metadata.update(brands_metadata)
        # print the conversation payload
        print(ujson.dumps(conversation_with_metadata))
        #hydrated_conversations.append(conversation_with_metadata) #debugging
    logging.debug('{} shards have been processed. There are {} shards remaining.'.format(item + 1, shard_number - item - 1))

##################################################################################### Cleanup

# Close the database
tweet_collection.drop()
client.drop_database('tweet_database')
client.close()

logging.debug('Cleaned up the database (deleted the database & collection, closed the client)')


