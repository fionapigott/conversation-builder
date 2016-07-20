# Author: Fiona Pigott
# Date: July 13, 2016
# Free to use, no guarantees of anything

from snowflake2utc import snowflake2utc
import field_getters as fg

'''
All of these functions calculate some aggregate statistic about the hydrated_conversation object
A hydrated_converstaion is a time-ordered list of Tweets payloads and relevant data, with exactly these keys and data:
e.g.:
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

def size_of_conversation(hydrated_conversation):
    '''Length of the hydrate conversation object'''
    return len(hydrated_conversation)

def approx_depth(hydrated_conversation):
    '''Depth of the reply chain'''
    return max([x["depth"] for x in hydrated_conversation])

def tweets(hydrated_conversation):
    '''List of Tweet payloads in the hydrated_conversation'''
    return [x["tweet"] for x in hydrated_conversation]

def root_user(hydrated_conversation):
    '''User who initiated the conversation'''
    return {"screen_name": hydrated_conversation[0]["screen_name"], "user_id": hydrated_conversation[0]["user_id"]}

def time_to_first_response(hydrated_conversation):
    '''
    Time (in the format <zero-padded hours>:<zero-padded minutes:<zero-padded seconds>, H:M:S)
    between the first Tweet in the thread and the next Tweet by a different user.
    "NO_RESPONSE" if there is only one user in the thread
    '''
    if len(hydrated_conversation) > 1:
        root_user_id = hydrated_conversation[0]["user_id"]
        root_screen_name = hydrated_conversation[0]["screen_name"]
        root_tweet_time = snowflake2utc(hydrated_conversation[0]["id"])
        for t in hydrated_conversation:
            if (t["user_id"] != root_user_id) and (t["screen_name"] != root_screen_name):
                seconds = (snowflake2utc(t["id"]) - root_tweet_time)
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                return "{}:{}:{}".format(str(h).zfill(2),str(m).zfill(2),str(s).zfill(2))
    else:
        return "NO_RESPONSE"

def time_to_first_brand_response(hydrated_conversation, brands):
    '''
    Time (in the format <zero-padded hours>:<zero-padded minutes:<zero-padded seconds>, H:M:S)
    between the first Tweet in the thread and the next Tweet by a brand user (brands are specifed in the argument 'brands').
    "NO_RESPONSE" if no brand responds in the thread
    '''
    if len(hydrated_conversation) > 1:
        brands_ids = [b["user_id"] for b in brands]
        brands_names = [b["screen_name"] for b in brands]
        root_user_id = hydrated_conversation[0]["user_id"]
        root_screen_name = hydrated_conversation[0]["screen_name"]
        root_tweet_time = snowflake2utc(hydrated_conversation[0]["id"])
        if (root_user_id in brands_ids) or (root_screen_name in brands_names):
            return "UNDEFINED"
        for t in hydrated_conversation:
            if (t["user_id"] in brands_ids) or (t["screen_name"] in brands_names):
                seconds = (snowflake2utc(t["id"]) - root_tweet_time)
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                return "{}:{}:{}".format(str(h).zfill(2),str(m).zfill(2),str(s).zfill(2))
        return "NO_RESPONSE"
    else:
        return "NO_RESPONSE"

def duration_of_conversation(hydrated_conversation):
    '''
    Duration (in the format <zero-padded hours>:<zero-padded minutes:<zero-padded seconds>, H:M:S)
    between the first Tweet in the thread and the last Tweet in the thread, whether or not there is more than one user.
    "NO_RESPONSE" if there is only one Tweet in the thread
    '''
    if len(hydrated_conversation) > 1:
        root_tweet_time = snowflake2utc(hydrated_conversation[0]["id"])
        last_tweet_time = snowflake2utc(hydrated_conversation[-1]["id"])
        seconds = last_tweet_time - root_tweet_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return "{}:{}:{}".format(str(h).zfill(2),str(m).zfill(2),str(s).zfill(2))
    else:
        return "NO_RESPONSE"

def first_brand_response(hydrated_conversation, brands):
    '''
    Tweet paylaod of the first brand to respond in the thread.
    "UNDEFINED" if the first Tweet is by a brand.
    "NO_RESPONSE" if there is no brand in the thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    root_tweet = hydrated_conversation[0]
    if len(hydrated_conversation) > 1:
        if (root_tweet["user_id"] not in brands_ids) and (root_tweet["screen_name"] not in brands_names):
            for tweet in hydrated_conversation[1:]:
                if (tweet["user_id"] in brands_ids) or (tweet["screen_name"] in brands_names):
                    return tweet["tweet"]
        else:
            return "UNDEFINED"
        return "NO_RESPONSE"
    else:
        if (root_tweet["user_id"] not in brands_ids) and (root_tweet["screen_name"] not in brands_names):
            return "NO_RESPONSE"
        else:
            return "UNDEFINED"

def brands_tweeting(hydrated_conversation, brands):
    '''
    Users in the 'brands' list who Tweeted in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    brands_tweeting = []
    for x in hydrated_conversation:
        user = (x["user_id"], x["screen_name"])
        if (user[0] in brands_ids) or (user[1] in brands_names):
            brands_tweeting.append(user)
    return [{"screen_name": x[1], "user_id": x[0]} for x in set(brands_tweeting)]

def nonbrands_tweeting(hydrated_conversation, brands):
    '''
    Users not in the 'brands' list who Tweeted in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    non_brands_tweeting = []
    for x in hydrated_conversation:
        user = (x["user_id"], x["screen_name"])
        if (user[0] not in brands_ids) and (user[1] not in brands_names):
            non_brands_tweeting.append(user)
    return [{"screen_name": x[1], "user_id": x[0]} for x in set(non_brands_tweeting)]

def brands_mentioned(hydrated_conversation, brands):
    '''
    Users in the 'brands' list who are @ mentioned in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    users_mentioned = []
    for c in hydrated_conversation:
        try:
            users_mentioned.extend([(x["screen_name"].lower(), x["id_str"]) for x in fg.user_mentions(c["tweet"])])
        except KeyError:
            pass
    brands_mentioned = []
    for user in users_mentioned:
        if (user[1] in brands_ids) or (user[0] in brands_names):
            brands_mentioned.append(user)
    return [{"screen_name": x[0], "user_id": x[1]} for x in set(brands_mentioned)]

def nonbrands_mentioned(hydrated_conversation, brands):
    '''
    Users not in the 'brands' list who are @ mentioned in this thread
    '''
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    users_mentioned = []
    for c in hydrated_conversation:
        try:
            users_mentioned.extend([(x["screen_name"].lower(), x["id_str"]) for x in fg.user_mentions(c["tweet"])])
        except KeyError:
            pass
    non_brands_mentioned = []
    for user in users_mentioned:
        if (user[1] not in brands_ids) and (user[0] not in brands_names):
            non_brands_mentioned.append(user)
    return [{"screen_name": x[0], "user_id": x[1]} for x in set(non_brands_mentioned)]

def ids_of_missing_tweets(hydrated_conversation):
    '''
    Ids of the Tweets that appeared in the inREplyTo fields of other Tweets in the dataset, but were not present in the dataset
    '''
    return [x["id"] for x in hydrated_conversation if x["missing"]]

def depths(hydrated_conversation):
    '''
    List of depths, same order as the tweets list
    '''
    return [x["depth"] for x in hydrated_conversation]
