# Author: Fiona Pigott
# Date: July 13, 2016
# Free to use, no guarantees of anything

import pymongo
import fileinput
import ujson
import sys
import argparse
import logging
import field_getters as fg
from find_children import find_children
from create_database import create_database
from snowflake2utc import snowflake2utc
import add_enrichments
from get_brand_info import get_brand_info

def build_conversations(max_in_memory_value = 10000, database_filename = "-", db_name = "tweet_database", drop_if_nonempty = True):
    '''
    Function to organize Tweets into conversations 
    (conversations = list of Tweets linked by inReplyTo fields)

    The output from this script is a set of JSON payloads (1 per line) that contain:
    {
        "tweets": [  # time-sorted list of Tweets
            { < Tweet payload > }, # if the first Tweet was missing, it has the format: {"missing_tweet_id": _, "screen_name": _, "user_id": _}
            { < Tweet payload > }  
          ],  
        "depths": [0,1...] #List of depths, same order as the tweets list  
    }

    Tweets in a conversation are time-sorted.

    The output is intended to provide a way to group Tweets so that the user can do 
    a row-level conversation analysis without having to hold more than 1 conversation's Tweets in memory.
    '''

    # get the logger
    logging.getLogger("root")
    ##################################################################################### Database creation step

    # store all of the Twets in a database with the following keys:
    # _id, in_reply_to_id, in_reply_to_user, in_reply_To_user_id
    client, db_name, tweet_collection = create_database(database_filename, db_name, drop_if_nonempty)

    ##################################################################################### Graph creation step

    # get links from parent to child nodes
    # the .aggregate function is provided by pymongo, as are the syntax/functions of this group step
    parent_to_children = {
        x["_id"]: {"children": x["children"], 
                   "in_reply_to_user": x["in_reply_to_user"], 
                   "in_reply_to_user_id": x["in_reply_to_user_id"] } 
          for x in tweet_collection.aggregate([
                # define pymongo group step for aggregating the database
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

    tweets_involved = len(set(all_children) | set(parent_to_children.keys()) - set(["NOT_A_REPLY"]))
    logging.debug('There are {} Tweets involved in the conversation'.format(tweets_involved) + 
       ' (some Tweets appear in an "inReplyTo" field, so we know they existed, ' + 
       'but they were not in the dataset)')

    # keys that are not children + tweets that are not replies
    root_nodes = (set(parent_to_children.keys()) - set(["NOT_A_REPLY"]) - 
        set(all_children)) | (set(parent_to_children["NOT_A_REPLY"]["children"])) 
    logging.debug('There are {} individual conversation threads.'.format(len(root_nodes)))
    del(all_children)

    # all of the conversation graphs
    multi_node_graphs = []
    # group the tweets together in conversations
    for root in root_nodes:
        children = find_children(root, None, 0, parent_to_children)
        multi_node_graphs.append(sorted(children, key=lambda k: k["depth"]))

    # in case of missing tweets, we want some info about the originating user
    tweet_to_screenname = {k: {"user_id": v["in_reply_to_user_id"], 
                               "screen_name": v["in_reply_to_user"]} for k,v in parent_to_children.items()}

    del(parent_to_children)
    del(root_nodes)

    logging.debug('Finished buiding the tree graph structure.')

    ##################################################################################### Graph hydration step
    # add the actual payloads of the Tweets and information about the graph structure to 
    # conversation objects

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
                    hydrated_conversation.append(
                        { 
                          "depth": tweet["depth"], 
                          "tweet": tweet_dict
                        }
                        )
                except KeyError:
                    # if it's not a Tweet in our dataset
                    hydrated_conversation.append(
                        {
                          "depth": tweet["depth"],
                          "tweet": {"missing_tweet_id": tweet["tweet_id"], 
                                    "screen_name": tweet_to_screenname[tweet["tweet_id"]]["screen_name"],
                                    "user_id": tweet_to_screenname[tweet["tweet_id"]]["user_id"]}
                        }
                        )
            # time-sort the conversation. 
            hydrated_conversation_sorted = sorted(hydrated_conversation, 
                key = lambda x: snowflake2utc(fg.tweet_id(x["tweet"])))
            conversation_payload = {"depths": [x["depth"] for x in hydrated_conversation_sorted], 
                                    "tweets": [x["tweet"] for x in hydrated_conversation_sorted]}
            # print the conversation payload
            #print(ujson.dumps(conversation_payload))
            yield(conversation_payload)
            #hydrated_conversations.append(hydrated_conversation) #debugging
        logging.debug('{} shards have been processed. There are {} shards remaining.'.format(item + 1, shard_number - item - 1))

    ##################################################################################### Cleanup

    # Close the database
    tweet_collection.drop()
    client.drop_database(db_name)
    client.close()

    logging.debug('Cleaned up the database (deleted the database & collection, closed the client)')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--max_in_memory_value', type = int, default = 10000, 
        help='maximum number of Tweets to hold in memory at a single time, default 10k')
    parser.add_argument('--log', default = 'build_conversations.log', help='name of log file')
    parser.add_argument('--input', default = '-', help='name of input Tweet data, default is stdin')
    parser.add_argument('--brand_info', default = None)
    parser.add_argument('--add_enrichments', action='store_true', help='add (or update) enrichment fields to these conversations')
    args = parser.parse_args()

    logging.basicConfig(filename=args.log,level=logging.DEBUG, format='%(asctime)s: In file: %(name)s, On line %(lineno)d: %(message)s')
    logging.debug('###################################################################### ' + 
        'building a new set of conversations')

    # you should never have to change these, put I'm putting them here for visibility, 
    # and making them accessible in case you do have to change them
    db_name = "tweet_database"
    drop_if_nonempty = True

    # add brand enrichments
    if args.brand_info is not None:
        do_brand_enrichments = True
        brands = get_brand_info(args.brand_info) 
    else:
        do_brand_enrichments = False

    for conversation_payload in build_conversations(args.max_in_memory_value, args.input, db_name, drop_if_nonempty):
        # optionally update enrichments in the conversation payload
        if args.add_enrichments:
            # add enrichments
            conversation_payload = add_enrichments.add_enrichments(conversation_payload)
            if do_brand_enrichments:
                conversation_payload = add_enrichments.add_brand_enrichments(conversation_payload, brands)
        print(ujson.dumps(conversation_payload))


