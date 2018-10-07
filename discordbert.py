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
  db, cur = get_dbcon()
  cur.execute("SELECT `value` FROM `options` WHERE `convid` = %s AND `option` = %s", (convid, option))
  row = cur.fetchone()
  if row != None:
    options[(convid, option)] = row[0]
    return row[0]
  else:
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
  await client.change_presence(game=discord.Game(name='Say my name or !help'))
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
  if o == 'reply_prob':
    if re.match(r'^([0-9]+|[0-9]*\.[0-9]+)$', v):
      return True
    else:
      return False
  else:
    return False


def should_reply(si, sn, ci, cn, ui, un, txt, server, channel):
  member = None
  if server:
    member = server.get_member(client.user.id)

  if ui == client.user.id:
    return False
  if channel and member and (not channel.permissions_for(member).send_messages):
    return False
  if txt and (Config.get('Chat', 'Keyword') in txt.lower()):
    return True
#  if not cn:
#    return True
  if txt and member and member.nick:
    if member.nick.lower() in txt.lower():
      return True
  prob = option_get_float(si, ci, 'reply_prob', 1, 0)
  if (uniform(0, 1) < prob):
    return True
  return False

helpstring="""I'm just a wolf! Talk to me, I answer when you say my name. You can also change my nickname on your server.
Type *!help* to show this text.
Type *!set reply_prob P* to set reply probability in current chat to P (0 - 1.0).
Click this to add me to your server: https://discordapp.com/oauth2/authorize?client_id=477996444775743488&scope=bot
"""

cmd_replies = set()

@client.event
async def on_message(message):
  ci, cn = channelidname(message.channel)
  si, sn = serveridname(message.server)
  ui = message.author.id
  un = message.author.name
  txt = message.content

  print('%s/%s %s/%s %s/%s : %s' % (sn, si, cn, ci, message.author.name, message.author.id, txt))
  if message.id in cmd_replies:
    print('(not logging)')
    return
  log_chat(si, sn, ci, cn, ui, un, 0, txt)

  if txt.startswith('!help'):
    cmd_replies.add((await client.send_message(message.channel, helpstring)).id)
  elif txt.startswith('!set '):
    splt = txt.split()
    if (len(splt) != 3):
      cmd_replies.add((await client.send_message(message.channel, "< invalid syntax, use !set option value >")).id)
      return
    opt = splt[1]
    val = splt[2]
    if option_valid(opt, val):
      option_set(ci, opt, val)
      cmd_replies.add((await client.send_message(message.channel, "< option %s set to %s >" % (opt, val))).id)
    else:
      cmd_replies.add((await client.send_message(message.channel, "< invalid option or value >")).id)
  elif txt.startswith('!clear'):
    options.clear()
    print('options cache flushed')
  else:
    put(ci, txt)
    if should_reply(si, sn, ci, cn, ui, un, txt, message.server, message.channel):
      await client.send_typing(message.channel)
      rpl = await asyncio.get_event_loop().run_in_executor(None, lambda: get(ci))
      await client.send_message(message.channel, rpl)
    convclean()

client.run(Config.get('Discord', 'Token'))
