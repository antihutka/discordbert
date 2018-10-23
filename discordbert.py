import discord
import asyncio
import logging
from configparser import ConfigParser
import MySQLdb
import socket
import sys
import traceback
from time import time, sleep
import re
from random import uniform


Config = ConfigParser()
Config.read(sys.argv[1])

logging.basicConfig(level=logging.INFO)

def get_dbcon():
  db = MySQLdb.connect(host=Config.get('Database', 'Host'), user=Config.get('Database', 'User'), passwd=Config.get('Database', 'Password'), db=Config.get('Database', 'Database'), charset='utf8')
  cur = db.cursor()
  cur.execute('SET NAMES utf8mb4')
  return db, cur

def log_chat(si, sn, ci, cn, ui, un, sent, message):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat` (`server_id`, `server_name`, `channel_id`, `channel_name`, `user_id`, `user_name`, `sent`, `message`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (si, sn, ci, cn, ui, un, sent, message))
  db.commit()
  db.close()

options = {}

def option_set(convid, option, value):
  db, cur = get_dbcon()
  cur.execute("REPLACE INTO `options` (`convid`, `option`, `value`) VALUES (%s,%s, %s)", (convid, option, str(value)))
  db.commit()
  db.close()
  options[(convid, option)] = value

def option_get_raw(convid, option):
  if (convid, option) in options:
    return options[(convid, option)]
  print('raw getting option %s %s' % (convid, option))
  db, cur = get_dbcon()
  cur.execute("SELECT `value` FROM `options` WHERE `convid` = %s AND `option` = %s", (convid, option))
  row = cur.fetchone()
  if row != None:
    options[(convid, option)] = row[0]
    return row[0]
  else:
    options[(convid, option)] = None
    return None

def option_get_float(serverid, convid, option, def_u, def_g):
  try:
    oraw = option_get_raw(convid, option)
    if oraw != None:
      return float(oraw)
  except Exception as e:
    print("Error getting option %s for conv %d: %s" % (option, convid, str(e)))
  if serverid:
    return def_g
  else:
    return def_u


convos = {}
times = {}

def getconv(convid):
  if convid not in convos:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((Config.get('Backend', 'Host'), Config.getint('Backend', 'Port')))
    f = s.makefile()
    convos[convid] = (s,f)
  times[convid] = time()
  return convos[convid]

def convclean():
  now = time()
  for convid in times:
    if (convid in convos) and (times[convid] + Config.getfloat('Chat', 'Timeout') * 60 * 60 < now):
      print('Deleting conversation %s' % (convid,))
      s = convos[convid][0]
      s.shutdown(socket.SHUT_RDWR)
      s.close()
      convos[convid][1].close()
      del convos[convid]

def put(convid, text):
  if text == '':
    return
  text = re.sub('[\r\n]+', '\n',text).strip("\r\n")
  try:
    (s, f) = getconv(convid)
    s.send((text + '\n').encode('utf-8', 'ignore'))
  except Exception as e:
    traceback.print_exc(file=sys.stdout)
    del convos[convid]

def get(convid):
  try:
    (s, f) = getconv(convid)
    s.send('\n'.encode('utf-8'))
    return f.readline().rstrip()
  except Exception as e:
    traceback.print_exc(file=sys.stdout)
    del convos[convid]
    return ''

client = discord.Client()

@client.event
async def on_ready():
  print('Logged in as')
  print(client.user.name)
  print(client.user.id)
  print('------')
  print('Trying to change presence')
  await client.change_presence(game=discord.Game(name='Say my name or /!help'))
  print('Done')

def serveridname(server):
  if server:
    return (server.id, server.name)
  else:
    return (None, None)

def channelidname(channel):
  if channel:
    return (channel.id, channel.name)
  else:
    return (None, None)

def option_valid(o, v):
  if o in ['reply_prob', 'max_bot_msg_length', 'mention_only', 'prefix_only']:
    if re.match(r'^([0-9]+|[0-9]*\.[0-9]+)$', v):
      return True
    else:
      return False
  else:
    return False


def should_reply(si, sn, ci, cn, ui, un, txt, server, channel, author):
  opt_mention_only = option_get_float(si, ci, 'mention_only', 0, 1)
  keywords = ["<@%s>" % client.user.id, "<@!%s>" % client.user.id]

  # ignore empty messages
  if not txt:
    return False

  # never reply to own messages
  if ui == client.user.id:
    return False

  # ignore bots by default
  if author.bot and option_get_float(si, ci, 'reply_to_bots', 0, 0) == 0:
    return False

  member = None
  if server:
    member = server.get_member(client.user.id)

  # check send perms
  if channel and member and (not channel.permissions_for(member).send_messages):
    return False

  if opt_mention_only <= 0:
    keywords.append(Config.get('Chat', 'Keyword').lower())
    if member and member.nick:
      keywords.append(member.nick.lower())

#  print("kw: ", keywords)
  if option_get_float(si, ci, 'prefix_only', 0, 1) <= 0:
    for kw in keywords:
      if kw in txt.lower():
        return True
  else:
    for kw in keywords:
      if txt.lower().startswith(kw):
        return True

  prob = option_get_float(si, ci, 'reply_prob', 1, 0)
  if (uniform(0, 1) < prob):
    return True
  return False

help_links="""[Add me to your server](https://discordapp.com/oauth2/authorize?client_id=477996444775743488&scope=bot)
[Support server](https://discord.gg/EhNr4hR)
[DBL link](https://discordbots.org/bot/477996444775743488)"""

def make_help():
  emb = discord.Embed(description="Sobert's silly help thing")
  emb.add_field(name="/!help", value="Show this text")
  emb.add_field(name="/!set reply_prob P", value="Set my reply probability for the current channel to P (0 to 1.0). Defaults to 0, except in DMs.")
  emb.add_field(name="/!set max_max_bot_msg_length L", value="Don't process messages from bots longer than L characters. Defaults to 200.")
  emb.add_field(name="/!set prefix_only 0|1", value="Only match keywords as prefixes, not anywhere in the message.")
  emb.add_field(name="/!set mention_only 0|1", value="Don't match on name, only @mention.")
  emb.add_field(name="Links and stuff", value=help_links)
  return emb

#Type */!set reply_to_bots 0|1* to enable or disable. Defaults to 0.

cmd_replies = set()

@client.event
async def on_message(message):
  start_time = time()
  msgcolor = ''
  ci, cn = channelidname(message.channel)
  si, sn = serveridname(message.server)
  ui = message.author.id
  un = message.author.name
  txt = message.content

  if txt == "":
    return

  channel_ignored = False
  if message.author.bot:
    msgcolor = '\033[34m'
  if option_get_float(si, ci, 'ignore_channel', 0, 0) > 0:
    channel_ignored = True
    msgcolor = '\033[90m'
  if not message.server:
    msgcolor = '\033[96m'
  if ui == client.user.id:
    msgcolor = '\033[92m'

  print(msgcolor + ('%s/%s %s/%s %s/%s : %s' % (sn, si, cn, ci, message.author.name, message.author.id, txt)) + '\033[0m')
  if message.id in cmd_replies:
    print('(not logging)')
    return
  await asyncio.get_event_loop().run_in_executor(None, lambda: log_chat(si, sn, ci, cn, ui, un, 0, txt))

  if channel_ignored == True:
    return

  if txt.startswith('/!') and message.author.bot:
    return

  if txt.startswith('/!help'):
    await client.send_message(message.channel, embed=make_help())
  elif txt.startswith('/!set '):
    if message.server and not message.author.permissions_in(message.channel).manage_channels:
      cmd_replies.add((await client.send_message(message.channel, "< only people with manage_channels permission can set options >")).id)
      return
    splt = txt.split()
    if (len(splt) != 3):
      cmd_replies.add((await client.send_message(message.channel, "< invalid syntax, use /!set option value >")).id)
      return
    opt = splt[1]
    val = splt[2]
    if option_valid(opt, val):
      option_set(ci, opt, val)
      cmd_replies.add((await client.send_message(message.channel, "< option %s set to %s >" % (opt, val))).id)
    else:
      cmd_replies.add((await client.send_message(message.channel, "< invalid option or value >")).id)
  elif txt.startswith('/!clear'):
    options.clear()
    print('options cache flushed')
  else:
    if (not message.author.bot) or (len(txt) <= option_get_float(si, ci, 'max_bot_msg_length', 200, 200)):
      put(ci, txt)
    if should_reply(si, sn, ci, cn, ui, un, txt, message.server, message.channel, message.author):
      await client.send_typing(message.channel)
      rpl_txt = await asyncio.get_event_loop().run_in_executor(None, lambda: get(ci))
      rpl_msg = await client.send_message(message.channel, rpl_txt)
      end_time = time()
      reply_delay = end_time - start_time
      if reply_delay > 20:
        await client.edit_message(rpl_msg, rpl_txt + ('\n*reply delayed by %f seconds*' % (reply_delay)))
        print('message took %f seconds to generate' % (reply_delay))
    convclean()

client.run(Config.get('Discord', 'Token'))
