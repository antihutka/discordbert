from cachetools import cached, TTLCache
from cachetools.keys import hashkey
from collections import namedtuple

from sobutils.database import with_cursor

ChatOption = namedtuple('ChatOption', 'name type settable default_user default_group contexts description')

options_list = [
  ChatOption(name='reply_prob',         type=float, settable=True,  default_user=1.0, default_group=0.0, contexts=('channel', 'server'),         description='Probability of replying to any text message'),
  ChatOption(name='mention_only',       type=int,   settable=True,  default_user=0,   default_group=1,   contexts=('channel', 'server'),         description="Only treat the bot's mention as a trigger instead of name"),
  ChatOption(name='prefix_only',        type=int,   settable=True,  default_user=0,   default_group=1,   contexts=('channel', 'server'),         description='Only recognize triggers as prefixes instead of anywhere in the message'),
  ChatOption(name='extra_prefix',       type=str,   settable=True,  default_user='',  default_group='',  contexts=('channel', 'server', 'user'), description='Trigger responses by an additional prefix'),
  ChatOption(name='max_bot_msg_length', type=int,   settable=True,  default_user=200, default_group=200, contexts=('channel', 'server', 'user'), description="Process (but don't respond to) messages shorter than N characters"),
  ChatOption(name='delete_after',       type=int,   settable=True,  default_user=30,   default_group=30,   contexts=('channel', 'server'),         description="Delete training data older than N days, default and maximum = 30"),
  ChatOption(name='reply_to_bots',      type=int,   settable=False, default_user=0,   default_group=0,   contexts=('channel', 'server', 'user'), description=''),
  ChatOption(name='ignore_channel',     type=int,   settable=False, default_user=0,   default_group=0,   contexts=('channel'),                   description=''),
  ChatOption(name='is_bad',             type=int,   settable=False, default_user=0,   default_group=0,   contexts=('channel'),                   description=''),
  ChatOption(name='is_hidden',          type=int,   settable=False, default_user=0,   default_group=0,   contexts=('channel'),                   description=''),
  ChatOption(name='blacklisted',        type=int,   settable=False, default_user=0,   default_group=0,   contexts=('channel'),                   description='')
]

options = { o.name : o for o in options_list }

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
    return opts[option_name]
  if server_id:
    return options[option_name].default_group
  return options[option_name].default_user

class OptionError(Exception):
  pass

@with_cursor
def set_option_db(cursor, channel_id, option_name, value_parsed):
  cursor.execute('INSERT INTO options2 (channel_id, ' + option_name + ') VALUES (%s,%s) ON DUPLICATE KEY UPDATE ' + option_name + ' = %s', (channel_id, value_parsed, value_parsed))

def set_option(channel_id, option_name, value, user_only = True):
  if ((option_name not in options) or
      (user_only and not options[option_name].settable)
     ):
    raise OptionError("Unknown option: %s" % repr(option_name))
  try:
    value_parsed = None if value is None else options[option_name].type(value)
  except ValueError:
    raise OptionError("Can't parse value %s as %s" % (repr(option_name), options[option_name].type.__name__))
  optioncache.pop(hashkey(channel_id), None)
  try:
    set_option_db(channel_id, option_name, value_parsed)
  except Exception as e:
    print("Error setting value: %s" % str(e))
    raise OptionError("Database error")
