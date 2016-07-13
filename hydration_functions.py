from snowflake2utc import snowflake2utc
import field_getters as fg

def size_of_conversation(hydrated_conversation):
    return len(hydrated_conversation)

def approx_depth(hydrated_conversation):
    return max([x["depth"] for x in hydrated_conversation])

def tweets(hydrated_conversation):
    return [x["tweet"] for x in hydrated_conversation]

def root_user(hydrated_conversation):
    #if hydrated_conversation[0]["missing"]:
    #    return {"screen_name": hydrated_conversation[0]["screen_name"], "user_id": "UNAVAILABLE"}
    #else:
    return {"screen_name": hydrated_conversation[0]["screen_name"], "user_id": hydrated_conversation[0]["user_id"]}

def time_to_first_response(hydrated_conversation):
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
    if len(hydrated_conversation) > 1:
        brands_ids = [b["user_id"] for b in brands]
        brands_names = [b["screen_name"] for b in brands]
        root_tweet = hydrated_conversation[0]
        if (root_tweet["user_id"] not in brands_ids) and (root_tweet["screen_name"] not in brands_names):
            for tweet in hydrated_conversation[1:]:
                if (tweet["user_id"] in brands_ids) or (tweet["screen_name"] in brands_names):
                    return tweet["tweet"]
        else:
            return "UNDEFINED"
        return "NO_RESPONSE"
    else:
        return "NO_RESPONSE"

def brands_tweeting(hydrated_conversation, brands):
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    brands_tweeting = []
    for x in hydrated_conversation:
        user = (x["user_id"], x["screen_name"])
        if (user[0] in brands_ids) or (user[1] in brands_names):
            brands_tweeting.append(user)
    return [{"screen_name": x[1], "user_id": x[0]} for x in set(brands_tweeting)]

def nonbrands_tweeting(hydrated_conversation, brands):
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    non_brands_tweeting = []
    for x in hydrated_conversation:
        user = (x["user_id"], x["screen_name"])
        if (user[0] not in brands_ids) and (user[1] not in brands_names):
            non_brands_tweeting.append(user)
    return [{"screen_name": x[1], "user_id": x[0]} for x in set(non_brands_tweeting)]

def brands_mentioned(hydrated_conversation, brands, format = True):
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    users_mentioned = []
    for c in hydrated_conversation:
        try:
            users_mentioned.extend([(x["screen_name"].lower(), x["id_str"]) for x in fg.user_mentions(c["tweet"], format)])
        except KeyError:
            pass
    brands_mentioned = []
    for user in users_mentioned:
        if (user[1] in brands_ids) or (user[0] in brands_names):
            brands_mentioned.append(user)
    return [{"screen_name": x[0], "user_id": x[1]} for x in set(brands_mentioned)]

def nonbrands_mentioned(hydrated_conversation, brands, format = True):
    brands_ids = [b["user_id"] for b in brands]
    brands_names = [b["screen_name"] for b in brands]
    users_mentioned = []
    for c in hydrated_conversation:
        try:
            users_mentioned.extend([(x["screen_name"].lower(), x["id_str"]) for x in fg.user_mentions(c["tweet"], format)])
        except KeyError:
            pass
    non_brands_mentioned = []
    for user in users_mentioned:
        if (user[1] not in brands_ids) and (user[0] not in brands_names):
            non_brands_mentioned.append(user)
    return [{"screen_name": x[0], "user_id": x[1]} for x in set(non_brands_mentioned)]

def ids_of_missing_tweets(hydrated_conversation):
    return [x["id"] for x in hydrated_conversation if x["missing"]]
