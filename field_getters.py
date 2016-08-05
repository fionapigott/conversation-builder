# Authoer: Fiona Pigott
# Date: July 13, 2016
# Free to use, no guarantees of anything

'''
Functions should have a defined return value for Twitter API payloads, GNIP API payloads, 
and for "missing tweets" (If a Tweet is missing, it has the format: {"missing_tweet_id": _, "screen_name": _, "user_id": _})
'''

def tweet_id(tweet):
    '''
    Get the Tweet ID as a string from an activity-streams or an original format Tweet.
    Works for activity-streams (when format = True) or original format (when format = False)
    '''
    if ("postedTime" in tweet):
        return tweet["id"].split(":")[-1]
    elif ("created_at" in tweet):
        return tweet["id_str"]
    else:
        return tweet["missing_tweet_id"]

def user_id(tweet):
    '''
    Get the user ID as a string from an activity-streams or an original format Tweet.
    Works for activity-streams (when format = True) or original format (when format = False)
    '''
    if ("postedTime" in tweet):
        return tweet["actor"]["id"].split(":")[-1]
    elif ("created_at" in tweet):
        return tweet["user"]["id_str"]
    elif ("missing_tweet_id" in tweet):
        return tweet["user_id"]

def screen_name(tweet):
    '''
    Get the user screen name from an activity-streams or an original format Tweet.
    Works for activity-streams (when format = True) or original format (when format = False)
    '''
    if ("postedTime" in tweet):
        return tweet["actor"]["preferredUsername"].lower()
    elif ("created_at" in tweet):
        return tweet["user"]["screen_name"].lower()
    elif ("missing_tweet_id" in tweet):
        return tweet["screen_name"]

def reply_info(tweet):
    '''
    Get information about the Tweet this Tweet is replying to, return a dictionary
    of reply_id (id of Tweet being replied to), reply_user (screen name of user being replied to) and
    reply_user_id (user id of user being replied to).
    Works for activity-streams (when format = True) or original format (when format = False)
    '''
    if ("postedTime" in tweet):
        try:
            reply_id = tweet["inReplyTo"]["link"].split("/")[-1]
            reply_user = tweet["inReplyTo"]["link"].split("/")[-3].strip("\\").lower()
            reply_user_id = "UNAVAILABLE"
        except KeyError:
            reply_id = "NOT_A_REPLY"
            reply_user = "NOT_A_REPLY"
            reply_user_id = "NOT_A_REPLY"
    elif ("created_at" in tweet):
        if tweet["in_reply_to_status_id_str"] is not None:
            reply_id = tweet["in_reply_to_status_id_str"]
            reply_user = tweet["in_reply_to_screen_name"].lower()
            reply_user_id = tweet["in_reply_to_user_id_str"]
        else:
            reply_id = "NOT_A_REPLY"
            reply_user = "NOT_A_REPLY"
            reply_user_id = "NOT_A_REPLY"
    else:
        reply_id = "UNAVAILABLE"
        reply_user = "UNAVAILABLE"
        reply_user_id = "UNAVAILABLE"
    return {"reply_id": reply_id, "reply_user": reply_user, "reply_user_id": reply_user_id}

def user_mentions(tweet):
    '''
    Get the list of user mentions.
    Works for activity-streams (when format = True) or original format (when format = False)
    '''
    if ("postedTime" in tweet):
        return tweet["twitter_entities"]["user_mentions"]
    elif ("created_at" in tweet):
        return tweet["entities"]["user_mentions"]
    else:
        return []
