from cachetools import cached, TTLCache
from cachetools.keys import hashkey

from sobutils.database import with_cursor

user_options = ['reply_prob', 'mention_only', 'prefix_only', 'extra_prefix', 'max_bot_msg_length']

option_types = {
  'reply_prob': float,
  'mention_only': int,
  'prefix_only': int,
  'extra_prefix': str,
  'max_bot_msg_length' : int,
  'reply_to_bots' : int,
  'ignore_channel': int,
  'is_bad': int,
  'is_hidden': int,
  'blacklisted': int
}

default_user = {
  'reply_prob': 1,
  'mention_only': 0,
  'prefix_only': 0,
  'extra_prefix': '',
  'max_bot_msg_length' : 200,
  'reply_to_bots' : 0,
  'ignore_channel': 0,
  'is_bad': 0,
  'is_hidden': 0,
  'blacklisted': 0
}

default_group = {
  'reply_prob': 0,
  'mention_only': 1,
  'prefix_only': 1,
  'extra_prefix': '',
  'max_bot_msg_length' : 200,
  'reply_to_bots' : 0,
  'ignore_channel': 0,
  'is_bad': 0,
  'is_hidden': 0,
  'blacklisted': 0
}

optioncache = TTLCache(1024, 60*60)

@cached(optioncache)
@with_cursor
def get_all_options(cursor, channel_id):
  ret = {}
  cursor.execute('SELECT * FROM options2 WHERE channel_id=%s', (channel_id,))
  r = cursor.fetchone()
  if r:
    for (desc, val) in zip(cursor.description, r):
      if val is not None:
        ret[desc[0]] = val
  print('Fetched options for %d => %s' % (channel_id, ret))
  return ret

def get_option(server_id, channel_id, option_name):
  opts = get_all_options(channel_id)
  if option_name in opts:
    #print('Returning set option %s %s -> %s' % (channel_id, option_name, opts[option_name]))
    return opts[option_name]
  if server_id:
    #print('Returning group default %s %s -> %s' % (channel_id, option_name, default_group[option_name]))
    return default_group[option_name]
  #print('Returning user default %s %s -> %s' % (channel_id, option_name, default_user[option_name]))
  return default_user[option_name]

class OptionError(Exception):
  pass

@with_cursor
def set_option_db(cursor, channel_id, option_name, value_parsed):
  cursor.execute('INSERT INTO options2 (channel_id, ' + option_name + ') VALUES (%s,%s) ON DUPLICATE KEY UPDATE ' + option_name + ' = %s', (channel_id, value_parsed, value_parsed))

def set_option(channel_id, option_name, value, user_only = True):
  if ((option_name not in option_types) or
      (user_only and option_name not in user_options)
     ):
    raise OptionError("Unknown option: %s" % repr(option_name))
  try:
    value_parsed = None if value is None else option_types[option_name](value)
  except ValueError:
    raise OptionError("Can't parse value %s as %s" % (repr(option_name), option_types[option_name].__name__))
  optioncache.pop(hashkey(channel_id), None)
  try:
    set_option_db(channel_id, option_name, value_parsed)
  except Exception as e:
    print("Error setting value: %s" % str(e))
    raise OptionError("Database error")
