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
    return lambda: f.readline().rstrip()
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
  if not cn:
    return True
  if txt and member and member.nick:
    if member.nick.lower() in txt.lower():
      return True
  return False

helpstring="""I'm just a wolf! Talk to me, I answer when you say my name. You can also change my nickname on your server.
Type !help to show this text.
Click this to add me to your server: https://discordapp.com/oauth2/authorize?client_id=477996444775743488&scope=bot
"""

@client.event
async def on_message(message):
  ci, cn = channelidname(message.channel)
  si, sn = serveridname(message.server)
  ui = message.author.id
  un = message.author.name
  txt = message.content

  print('%s/%s %s/%s %s/%s : %s' % (sn, si, cn, ci, message.author.name, message.author.id, txt))
  log_chat(si, sn, ci, cn, ui, un, 0, txt)

  put(ci, txt)
  if should_reply(si, sn, ci, cn, ui, un, txt, message.server, message.channel):
    rpl = get(ci)()
    await client.send_message(message.channel, rpl)
  convclean()

  if txt.startswith('!help'):
    await client.send_message(message.channel, helpstring)
#  if txt.startswith('!test'):
#    counter = 0
#    tmp = await client.send_message(message.channel, 'Calculating messages...')
#    async for log in client.logs_from(message.channel, limit=100):
#      if log.author == message.author:
#        counter += 1
#    await client.edit_message(tmp, 'You have {} messages.'.format(counter))
#  elif txt.startswith('!sleep'):
#    await asyncio.sleep(5)
#    await client.send_message(message.channel, 'Done sleeping')

client.run(Config.get('Discord', 'Token'))
