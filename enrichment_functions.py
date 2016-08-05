# Author: Fiona Pigott
# Date: July 13, 2016
# Free to use, no guarantees of anything

from snowflake2utc import snowflake2utc
import field_getters as fg

'''
All of these functions calculate some aggregate statistic about the conversation_payload object
A conversation_payload has at least these keys and data:

    {
        "tweets": [  # time-sorted list of Tweets
            { < Tweet payload > }, 
            { < Tweet payload > }  
          ],  
        "depths": [0,1...] #List of depths, same order as the tweets list  
    }

If a Tweet is missing, it has the format: {"missing_tweet_id": _, "screen_name": _, "user_id": _}

Some of these functions expect a "brands" input, which is a list of dictionaries of brands' ids and screen names:
[ {"screen_name": brand1, "user_id": 11111}, {"screen_name": brand2, "user_id": 22222}, ... ]
'''

def size_of_conversation(conversation_payload):
    '''Length of the hydrate conversation object'''
    return len(conversation_payload["tweets"])

def approx_depth(conversation_payload):
    '''Depth of the reply chain'''
    return max(conversation_payload["depths"])

def root_user(conversation_payload):
    '''User who initiated the conversation'''
    return {
        "screen_name": fg.screen_name(conversation_payload["tweets"][0]), 
        "user_id": fg.user_id(conversation_payload["tweets"][0])
        }

def time_to_first_response(conversation_payload):
    '''
    Time (in the format <zero-padded hours>:<zero-padded minutes:<zero-padded seconds>, H:M:S)
    between the first Tweet in the thread and the next Tweet by a different user.
    "NO_RESPONSE" if there is only one user in the thread
    '''
    if len(conversation_payload) > 1:
        root_user_id = fg.user_id(conversation_payload["tweets"][0])
        root_screen_name = fg.screen_name(conversation_payload["tweets"][0])
        root_tweet_time = snowflake2utc(fg.tweet_id(conversation_payload["tweets"][0]))
        for t in conversation_payload["tweets"]:
            if (fg.user_id(t) != root_user_id) and (fg.screen_name(t) != root_screen_name):
                seconds = (snowflake2utc(fg.tweet_id(t)) - root_tweet_time)
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                return "{}:{}:{}".format(str(h).zfill(2),str(m).zfill(2),str(s).zfill(2))
    else:
        return "NO_RESPONSE"

def time_to_first_brand_response(conversation_payload, brands):
    '''
    Time (in the format <zero-padded hours>:<zero-padded minutes:<zero-padded seconds>, H:M:S)
    between the first Tweet in the thread and the next Tweet by a brand user (brands are specifed in the argument 'brands').
    "NO_RESPONSE" if no brand responds in the thread
    '''
    if len(conversation_payload["tweets"]) > 1:
        brands_ids = [b["user_id"] for b in brands]
        brands_names = [b["screen_name"] for b in brands]
        root_user_id = fg.user_id(conversation_payload["tweets"][0])
        root_screen_name = fg.screen_name(conversation_payload["tweets"][0])
        root_tweet_time = snowflake2utc(fg.tweet_id(conversation_payload["tweets"][0]))
        if (root_user_id in brands_ids) or (root_screen_name in brands_names):
            return "UNDEFINED"
        for t in conversation_payload["tweets"]:
            if (fg.user_id(t) in brands_ids) or (fg.screen_name(t) in brands_names):
                seconds = (snowflake2utc(fg.tweet_id(t)) - root_tweet_time)
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                return "{}:{}:{}".format(str(h).zfill(2),str(m).zfill(2),str(s).zfill(2))
        return "NO_RESPONSE"
    else:
        return "NO_RESPONSE"

def duration_of_conversation(conversation_payload):
    '''
    Duration (in the format <zero-padded hours>:<zero-padded minutes:<zero-padded seconds>, H:M:S)
    between the first Tweet in the thread and the last Tweet in the thread, whether or not there is more than one user.
    "NO_RESPONSE" if there is only one Tweet in the thread
    '''
    if len(conversation_payload["tweets"]) > 1:
        root_tweet_time = snowflake2utc(fg.tweet_id(conversation_payload["tweets"][0]))
        last_tweet_time = snowflake2utc(fg.tweet_id(conversation_payload["tweets"][-1]))
        seconds = last_tweet_time - root_tweet_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return "{}:{}:{}".format(str(h).zfill(2),str(m).zfill(2),str(s).zfill(2))
    else:
        return "NO_RESPONSE"

def first_brand_response(conversation_payload, brands):
    '''
    Tweet paylaod of the first brand to respond in the thread.
    "UNDEFINED" if the first Tweet is by a brand.
    "NO_RESPONSE" if there is no brand in the thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    root_tweet = conversation_payload["tweets"][0]
    if len(conversation_payload["tweets"]) > 1:
        if (fg.user_id(root_tweet) not in brands_ids) and (fg.screen_name(root_tweet) not in brands_names):
            for tweet in conversation_payload["tweets"][1:]:
                if (fg.user_id(tweet) in brands_ids) or (fg.screen_name(tweet) in brands_names):
                    return tweet
        else:
            return "UNDEFINED"
        return "NO_RESPONSE"
    else:
        if (fg.user_id(root_tweet) not in brands_ids) and (fg.screen_name(root_tweet) not in brands_names):
            return "NO_RESPONSE"
        else:
            return "UNDEFINED"

def brands_tweeting(conversation_payload, brands):
    '''
    Users in the 'brands' list who Tweeted in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    brands_tweeting = []
    for x in conversation_payload["tweets"]:
        user = (fg.user_id(x), fg.screen_name(x))
        if (user[0] in brands_ids) or (user[1] in brands_names):
            brands_tweeting.append(user)
    return [{"screen_name": x[1], "user_id": x[0]} for x in set(brands_tweeting)]

def nonbrands_tweeting(conversation_payload, brands):
    '''
    Users not in the 'brands' list who Tweeted in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    non_brands_tweeting = []
    for x in conversation_payload["tweets"]:
        user = (fg.user_id(x), fg.screen_name(x))
        if (user[0] not in brands_ids) and (user[1] not in brands_names):
            non_brands_tweeting.append(user)
    return [{"screen_name": x[1], "user_id": x[0]} for x in set(non_brands_tweeting)]

def brands_mentioned(conversation_payload, brands):
    '''
    Users in the 'brands' list who are @ mentioned in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    users_mentioned = []
    for t in conversation_payload["tweets"]:
        try:
            users_mentioned.extend([(x["screen_name"].lower(), x["id_str"]) for x in fg.user_mentions(t)])
        except KeyError:
            pass
    brands_mentioned = []
    for user in users_mentioned:
        if (user[1] in brands_ids) or (user[0] in brands_names):
            brands_mentioned.append(user)
    return [{"screen_name": x[0], "user_id": x[1]} for x in set(brands_mentioned)]

def nonbrands_mentioned(conversation_payload, brands):
    '''
    Users not in the 'brands' list who are @ mentioned in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    users_mentioned = []
    for t in conversation_payload["tweets"]:
        try:
            users_mentioned.extend([(x["screen_name"].lower(), x["id_str"]) for x in fg.user_mentions(t)])
        except KeyError:
            pass
    non_brands_mentioned = []
    for user in users_mentioned:
        if (user[1] not in brands_ids) and (user[0] not in brands_names):
            non_brands_mentioned.append(user)
    return [{"screen_name": x[0], "user_id": x[1]} for x in set(non_brands_mentioned)]

def ids_of_missing_tweets(conversation_payload):
    '''
    Ids of the Tweets that appeared in the inReplyTo fields of other Tweets in the dataset, but were not present in the dataset
    '''
    return [fg.tweet_id(x) for x in conversation_payload["tweets"] if "missing_tweet_id" in x]

