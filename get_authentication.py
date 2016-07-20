# Author: Fiona Pigott
# Date: July 20, 2016
# Free to use, no guarantees of anything

import sys
import logging
import yaml
from requests_oauthlib import OAuth1
import requests
import os

def get_authentication(credentials_file):
    '''
    access the creds file, "credentials_file" is a path from your HOME directory
    I'm going to use the default .twurlrc setup for a credentials file. 
    If you already have a .twurlrc, it should work. Otherwise, create one. It should look like this:
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
    '''

    # get the logger
    logging.getLogger("root")

    creds = yaml.load(open(os.getenv('HOME') + "/" + credentials_file ,"r")) 
    keys = creds["profiles"][creds["configuration"]["default_profile"][0]][creds["configuration"]["default_profile"][1]]
    auth = OAuth1(keys["consumer_key"],keys["consumer_secret"],keys["token"],keys["secret"]) 
    
    return auth