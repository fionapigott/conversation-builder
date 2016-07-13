# Twitter Snowflake ID to timestamp (and back)
# https://github.com/client9/snowflake2time/
# Nick Galbreath @ngalbreath nickg@client9.com
# Public Domain -- No Copyright -- Cut-n-Paste!
def snowflake2utc(sf):
    return int(((sf >> 22) + 1288834974657) / 1000.0)