# Author: Fiona Pigott
# Date: July 13, 2016
# Free to use, no guarantees of anything

import pymongo
import fileinput
import ujson
from find_children import find_children
import sys
import argparse
import logging
import field_getters as fg
from create_database import create_database
from get_brand_info import get_brand_info
import add_metadata

##################################################################################### Parse args and set up logging

parser = argparse.ArgumentParser()
parser.add_argument('--brand_info', default = 'no brands', help='csv of brand screen name and brand id (e.g.: "notFromShrek,5555555")')
parser.add_argument('--max_in_memory_value', type = int, default = 10000, 
    help='maximum number of Tweets to hold in memory at a single time, default 10k')
parser.add_argument('--log', default = 'build_conversations.log', help='name of log file')
args = parser.parse_args()

logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: In file: %(name)s, On line %(lineno)d: %(message)s')
logging.debug('###################################################################### ' + 
    'building a new set of conversations')

# this could change depending on your RAM
max_in_memory_value = args.max_in_memory_value

##################################################################################### Get brand information step

brands = get_brand_info(args.brand_info)

##################################################################################### Database creation step

client, db_name, tweet_collection = create_database()

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
                hydrated_conversation.append({"screen_name": fg.screen_name(tweet_dict), 
                                              "user_id": fg.user_id(tweet_dict), 
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
                                              "tweet": {"missing_tweet_id": tweet["tweet_id"], 
                                                        "screen_name": tweet_to_screenname[tweet["tweet_id"]]["screen_name"],
                                                        "user_id": tweet_to_screenname[tweet["tweet_id"]]["user_id"]}
                                            })
        # calculate metadata about the conversation (add_metadata.py)
        conversation_with_metadata = add_metadata.add_metadata(hydrated_conversation, brands)
        # print the conversation payload
        print(ujson.dumps(conversation_with_metadata))
        #hydrated_conversations.append(conversation_with_metadata) #debugging
    logging.debug('{} shards have been processed. There are {} shards remaining.'.format(item + 1, shard_number - item - 1))

##################################################################################### Cleanup

# Close the database
tweet_collection.drop()
client.drop_database(db_name)
client.close()

logging.debug('Cleaned up the database (deleted the database & collection, closed the client)')


