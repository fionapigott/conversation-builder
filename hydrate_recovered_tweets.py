# Author: Fiona Pigott
# Date: July 20, 2016
# Free to use, no guarantees of anything

import ujson
import logging
import field_getters as fg
import add_metadata

def hydrate_recovered_tweets(convos_in_memory, recovered_tweets_dict, brands = []):
    '''
    Hydration step for recovered Tweets.
    The "depth" of a recovered Tweet is -1, because I'm not going to go through the trouble of building the tree structure again
    '''
    # hydrate the conversation
    # for each conversation that we currently have in memory (up to 100)
    for convo in convos_in_memory:
        hydrated_conversation = []
        depths = convo["depths"]
        new_missing_tweets = []
        recovered_tweets_ids = []
        unrecoverable_tweets = []
        # now we have to re-create the hydrated_conversation list
        for i,tweet_dict in enumerate(convo["tweets"]):
            # if the Tweet was never missing
            if "missing_tweet_id" not in tweet_dict:
                hydrated_conversation.append(
                   {"screen_name": fg.screen_name(tweet_dict), 
                    "user_id": fg.user_id(tweet_dict), 
                    "missing": False,
                    "depth": depths[i],
                    "in_reply_to": fg.reply_info(tweet_dict)["reply_id"],
                    "id": int(fg.tweet_id(tweet_dict)), 
                    "tweet": tweet_dict})
            # if the Tweet was missing
            else:
                # see if it was returned by the request (if not, it may have been deleted)
                try:
                    tweet_dict = recovered_tweets_dict[tweet_dict["missing_tweet_id"]]
                    reply_info = fg.reply_info(tweet_dict)
                    hydrated_conversation.append(
                       {"screen_name": fg.screen_name(tweet_dict), 
                        "user_id": fg.user_id(tweet_dict), 
                        "missing": False,
                        "depth": depths[i],
                        "in_reply_to": reply_info["reply_id"],
                        "id": int(fg.tweet_id(tweet_dict)), 
                        "tweet": tweet_dict})
                    recovered_tweets_ids.append(int(fg.tweet_id(tweet_dict)))
                    # if it was a reply we have another "missing" Tweet
                    if reply_info["reply_id"] != "NOT_A_REPLY":
                        hydrated_conversation.append(
                           {"screen_name": reply_info["reply_user"],
                            "user_id": reply_info["reply_user_id"],
                            "missing": True,
                            "depth": -1,
                            "id": int(reply_info["reply_id"]),
                            "tweet": {"missing_tweet_id": reply_info["reply_id"]}})
                        new_missing_tweets.append(int(reply_info["reply_id"]))
                # or just return another missing Tweet placeholder
                except KeyError:
                    hydrated_conversation.append(
                       {"screen_name": tweet_dict["screen_name"],
                        "user_id": tweet_dict["user_id"],
                        "missing": True,
                        "depth": depths[i],
                        "id": int(tweet_dict["missing_tweet_id"]),
                        "tweet": {"missing_tweet_id": tweet_dict["missing_tweet_id"]}})
                    unrecoverable_tweets.append(int(tweet_dict["missing_tweet_id"]))
        # calculate metadata about the conversation (add_metadata.py)
        conversation_with_metadata = add_metadata.add_metadata(hydrated_conversation, brands)
        # add a little bit more metadata about the Tweets that were recovered
        conversation_with_metadata.update({
            "recovered_tweets": recovered_tweets_ids, 
            "new_missing_tweets": new_missing_tweets, 
            "unrecoverable_tweets": unrecoverable_tweets})
        # print the conversation payload
        print(ujson.dumps(conversation_with_metadata))
