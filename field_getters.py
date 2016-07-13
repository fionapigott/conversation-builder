def tweet_id(tweet, format = True):
    if format:
        return tweet["id"].split(":")[-1]
    else:
        return tweet["id_str"]

def user_id(tweet, format = True):
    if format:
        return tweet["actor"]["id"].split(":")[-1]
    else:
        return tweet["user"]["id_str"]

def screen_name(tweet, format = True):
    if format:
        return tweet["actor"]["preferredUsername"].lower()
    else:
        return tweet["user"]["screen_name"].lower()

def reply_info(tweet, format = True):
    if format:
        try:
            reply_id = tweet["inReplyTo"]["link"].split("/")[-1]
            reply_user = tweet["inReplyTo"]["link"].split("/")[-3].strip("\\").lower()
            reply_user_id = "UNAVAILABLE"
        except KeyError:
            reply_id = "NOT_A_REPLY"
            reply_user = "NOT_A_REPLY"
            reply_user_id = "NOT_A_REPLY"
    else:
        if tweet["in_reply_to_status_id_str"] is not None:
            reply_id = tweet["in_reply_to_status_id_str"]
            reply_user = tweet["in_reply_to_screen_name"].lower()
            reply_user_id = tweet["in_reply_to_user_id_str"]
        else:
            reply_id = "NOT_A_REPLY"
            reply_user = "NOT_A_REPLY"
            reply_user_id = "NOT_A_REPLY"
    return {"reply_id": reply_id, "reply_user": reply_user, "reply_user_id": reply_user_id}

def user_mentions(tweet, format = True):
    if format:
        return tweet["twitter_entities"]["user_mentions"]
    else:
        return tweet["entities"]["user_mentions"]