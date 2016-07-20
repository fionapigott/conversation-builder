# Author: Fiona Pigott
# Date: July 19, 2016
# Free to use, no guarantees of anything

import fileinput
import logging

##################################################################################### Get brand information step

def get_brand_info(filename):

    # get the logger
    logging.getLogger("root")

    brands = []
    if filename is not None:
      for line in fileinput.input(filename):
          info = line.split(",")
          brands.append({"screen_name": info[0].strip().lower(), "user_id": info[1].strip()})

    logging.debug('The brands are: {}'.format([x["screen_name"] for x in brands]))

    return brands
