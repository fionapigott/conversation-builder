# Conversation builder for Tweet data
## Fiona Pigott

Python script to assemble individual Tweets from a public Twitter stream (either Gnip activity-streams format or original Twitter API format) into conversation threads, using the "in reply to" fields of the tweet payload.

This is still in active development, so bugs are very possible.

This package contains three stand-alone functions to create and "enrich" Tweet conversations and format them as "conversation payloads", where a "conversation payload" looks like:
    {
        "tweets": [  # time-sorted list of Tweets
            { < Tweet payload > }, # if the first Tweet was missing, it has the format: {"missing_tweet_id": _, "screen_name": _, "user_id": _}
            { < Tweet payload > }  
          ],  
        "depths": [0,1...] #List of depths, same order as the tweets list  
        < all other fields, "enrichment fields" go here >
    }

- build_conversations.py: Takes JSON formatted Tweet payloads, returns conversation payloads, optionally returns enriched conversation payloads with the --add_enrichments and --brand_info command line arguments 
- add_enrichments.py: Takes JSON formatted conversation payloads (with the "tweets" and "depths" fields at least) and adds enrichment fields. If you want to change the behaviour or types of enrichments, do it here, as this function is imported and runs when other code uses the --add_enrichments option
- add_missing_tweets.py: Takes JSON formatted conversation payloads (with the "tweets" and "depths" fields at least) and calls the Twitter public API to get the Tweets that were missing from the original dataset. Returns conversation payloads with added Tweets and a few extra fields about which Tweets were successfully returned from the API. Optionally enriches or updates enrichment fields with the --add_enrichments option

# build_conversations.py

**Input**: Stream of JSON-formatted Tweet payloads, one record per line.  
**Input** (optional): --brand_info: A CSV of brand handles and user ids, one handle/id pair per line, no quotes. E.g.: notFromShrek,2382763597)   
**Output**: Stream of JSON-formatted *conversation* payloads, one record per line. Ordering in the output will be pretty much random.  

## Output format:

A few notes about the output:
* User screen names are all converted to lower case. The only case where there are capital letters in the "screen_name field and "
* Tweets can be "missing" if a Tweet appears in an "in reply to" field, but not in the input dataset. In this case, we take the Tweet ID, user screen name, and the user id (user id of the user being replied to is only available in the original format). If Tweets are unavalible in the dataset, some fields will be undefined.
* All of the enrichment output fields that are brand-related will not appear if no --brand_info is provided
* The only different between activity-streams and original format output is that the "tweets" field will contain Tweet paylaods formatted in the same way as the input.

Output might look like this, when my provided --brand_info was:  
delta,5920532  
deltaassist,137460929  

And using the --add_enrichments option


    {
      "tweets": [  
        { < Tweet payload > }, # if the first Tweet was missing, it has the format: {"missing_tweet_id": _, "screen_name": _, "user_id": _} 
        { < Tweet payload > }  
      ],  
      "depths": [0,1...] #List of depths, same order as the tweets list 
      "nonbrands_tweeting": [{"screen_name": < screen name 1 >,"user_id": < user id 1 >}, {"screen_name": < screen name 2 >,"user_id": < user id 2 >}],  
      "nonbrands_mentioned": [{"screen_name": < screen name 1 >,"user_id": < user id 1 >}], # users who are mentioned in the thread, but are not brands  
      "time_to_first_response": "00:57:38", # H:M:S that it took for the first person to respond to this Tweet   
      "brands_mentioned": [{"screen_name": delta, "user_id": "5920532}], # Brands that were mentioned  
      "ids_of_missing_tweets": [ ... ], # this is a Tweet that we know was part of the conversation, but whose data was not in the input. empty if all Tweets were in the input  
      "size_of_conversation": 2, # number of Tweets in the conversation  

      "first_brand_response": {  
       < payload of the Tweet that was the brand's first response in this thread >   
      },  
      "brands_tweeting": [{"screen_name": "deltaassist","user_id": "137460929"}],  
      "time_to_first_brand_response": "00:57:38", # H:M:S that it took the brand to respond to this Tweet  
      "root_user": {"screen_name": < screen name 1 >,"user_id": < user id 1 >},  
      "approx_depth": 1, # Depth of the reply chain  
      "duration_of_conversation": "00:57:38" # (time of the last Tweet) - (time of the first Tweet),  
    }


## Usage

Remember to start the MongoDB daemon by running `mongod`.

`
cat raw_Tweet_data.json | python build_conversations.py --brand_info csv_of_brand_Twitter_handles_and_ids.csv --format activity-streams --log build_conversations.log > conversation_threads.json 
`

## Requirements:

* Python 3
* ujson (Python package for fast JSON encoding and decoding)
* MongoDB
* PyMongo (Python package for interfacing with MongoBD)

# Setup

## MongoDB
* On MacOSX, I was able to install MongoDB with the homebrew package manager (https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/). I also had to create a /data/db directoy, with a user:group fiona:staff.
* On a server running Ubuntu 14.04, I installed MongoDB following these instuctions (https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/) and it worked right away.

## Python 3
If you don't have Python 3, install that first. Among other things, Python 3's default UTF-8 character encoding is important for this usecase.
Then:
* `pip install ujson`
* `pip install pymongo`
(If you're not working in a Python virtual environment, you may have to `sudo pip install`)

The result of `pip freeze` for me is:  
    pymongo==3.3.0  
    ujson==1.35  

# add_enrichments.py

**Input**: Conversation payloads   
**Input** (optional): --brand_info: A CSV of brand handles and user ids, one handle/id pair per line, no quotes. E.g.: notFromShrek,2382763597)   
**Output**: Enriched conversation payloads   

Use this to add enrichments to the conversation payload. Will overwrite existing keys in the dictioary, will not delete keys that it is not overwriting. If you are building conversations and adding enrichments in one step, it is more efficient to use the build_conversations.py script with the --add_enrichments options (avoiding an extra JSON deserialization/serialization step).   

# add_missing_tweets.py

## Adding data from the public API:

### Requirements:
* requests
* requests_oauthlib
* pyyaml 

## add_missing_tweets.py

I've added a second script to add "missing" (Tweets that were not in the original dataset) back into the conversation paylaods called "add_missing_tweets.py"

This code depends on you having set up access to the Twitter Public API, so do that first (look up twurl for some instructions). I'm going to use the default .twurlrc setup for a credentials file. If you already have a .twurlrc, it should work. Otherwise, create one. It should look like this:
    --- 
    configuration: 
      default_profile: 
      - notFromShrek
      - < CONSUMER KEY >
    profiles: 
      notFromShrek: 
        < CONSUMER KEY >: 
          username: notFromShrek
          token: < TOKEN >
          secret: < SECRET >
          consumer_secret: < CONSUMER SECRET >
          consumer_key: < CONSUMER KEY >

The output is the same format as the conversation payloads above, with the addition of:
    {  
        "recovered_tweets": # list of Tweets ids from Tweets that were successfully returned by the API  
        "new_missing_tweets": # list of Tweets that we now know are missing, because the recovered Tweets were a reply to them (this script does not try to recover multiple levels of Tweet replies)  
        "unrecoverable_tweets": # list of missing Tweets that were not returned by the API  
    }  

Also, the "depth" of a recovered or newly missing Tweet is the depth of the Tweet it replied to -1, because I don't build the graph structure again. You can update the enrichment fields (make sure to do this, as conversation time and users involved may change) with --add_enrichments.   

The best way to get a complete graph structure is to pull data for missing Tweets, add them to your original dataset, and run the build_conversations.py script again, but sometimes that isn't feasible. This is a good, computationally cheaper solution.  

If you do want to simpy get raw Tweet data from the public API, I have an option --raw_data_only, which changes the behavior of the script to expecting raw Tweet IDs (unquoted Tweet IDs from stdin or from a file provided by --tweet_ids) and prints out the API response as a JSON payload.   

# Running the code

You can run this code as a pipeline in several different ways:

## To simply group Tweets into conversations:   
`cat some_Tweet_data.json | python build_conversations.py > conversation_output.json`  
## To group Tweets into conversations and add enrichments:   
`cat some_Tweet_data.json | python build_conversations.py --add_enrichments --brand_info csv_of_info_about_brands.csv >  enriched_conversation_output.json`  
## To add or update enrichments in some existing conversation payloads:   
`cat conversation_output.json | python add_enrichments.py --brand_info csv_of_info_about_brands.csv >  enriched_conversation_output.json`  
## To build conversation payloads, add back missing Tweets, then enrich data:
`cat some_Tweet_data.json | python build_conversations.py | python add_missing_tweets.py --add_enrichments --brand_info csv_of_info_about_brands.csv >  enriched_conversation_output_with_missing_tweets.json` 








