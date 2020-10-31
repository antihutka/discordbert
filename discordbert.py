import discord
import asyncio
import logging
import socket
import sys
import traceback
from time import time, sleep
import re
from random import uniform
from httpnn import HTTPNN
from queue import Queue
from cachetools import cached, LRUCache
from cachetools.keys import hashkey

from sobutils.configuration import Config
from sobutils.database import with_cursor, cache_on_commit
from sobutils.threads import start_thread
from sobutils.util import inqueue

import options

Config.read(sys.argv[1])

logging.basicConfig(level=logging.INFO)

nn = HTTPNN(Config.get('Backend', 'Url'), Config.get('Backend', 'Keyprefix'))
asyncio.get_event_loop().run_until_complete(nn.initialize())

logqueue = Queue()
start_thread(args=(logqueue, 'dblogger'))

channelinfo_cache = {}
channelinfo_current = {}
def log_channel(cur, channel):
  cid = channel.id
  cname = getattr(channel, 'name', None)
  cserver = channel.guild.id if hasattr(channel, 'guild') else None
  key = (cid, cname, cserver)
  if key in channelinfo_cache:
    infoid = channelinfo_cache[key]
  else:
    cur.execute("SELECT channelinfo_id FROM channelinfo WHERE channel_id = %s AND channel_name <=> %s AND server_id <=> %s LIMIT 1", key)
    res = cur.fetchall()
    if res:
      infoid = res[0][0]
      print('Known channel %s %d' % (key, infoid))
    else:
      cur.execute("INSERT INTO channelinfo (channel_id, channel_name, server_id) VALUES (%s, %s, %s)", key)
      infoid = cur.lastrowid
      print('New channel %s %d' % (key, infoid))
  cache_on_commit(cur, channelinfo_cache, key, infoid)
  if (cid not in channelinfo_current) or (channelinfo_current[cid] != infoid):
    print('Updating current channelinfo %d->%d' % (cid, infoid))
    cur.execute("REPLACE INTO channelinfo_current(channel_id, channelinfo_id) VALUES (%s, %s)", (cid, infoid))
    cache_on_commit(cur, channelinfo_current, cid, infoid)
  return infoid

userinfo_cache = {}
userinfo_current = {}
def log_user(cur, user):
  uid = user.id
  uname = user.name
  unick = getattr(user, 'nick', None)
  ubot = user.bot
  key = (uid, uname, unick, ubot)
  if key in userinfo_cache:
    infoid = userinfo_cache[key]
  else:
    cur.execute("SELECT userinfo_id FROM userinfo WHERE user_id = %s AND user_name = %s AND user_nick <=> %s AND is_bot = %s LIMIT 1", key)
    res = cur.fetchall()
    if res:
      infoid = res[0][0]
      print('Known user %s %d' % (key, infoid))
    else:
      cur.execute("INSERT INTO userinfo (user_id, user_name, user_nick, is_bot) VALUES (%s, %s, %s, %s)", key)
      infoid = cur.lastrowid
      print('New user %s %d' % (key, infoid))
  cache_on_commit(cur, userinfo_cache, key, infoid)
  if (uid not in userinfo_current) or (userinfo_current[uid] != infoid):
    print('Updating current userinfo %d->%d' % (uid, infoid))
    cur.execute("REPLACE INTO userinfo_current(user_id, userinfo_id) VALUES (%s, %s)", (uid, infoid))
    cache_on_commit(cur, userinfo_current, uid, infoid)
  return infoid

serverinfo_cache = {}
serverinfo_current = {}
def log_server(cur, server):
  if not server:
    return None
  sid = server.id
  sname = server.name
  key = (sid, sname)
  if key in serverinfo_cache:
    infoid = serverinfo_cache[key]
  else:
    cur.execute("SELECT serverinfo_id FROM serverinfo WHERE server_id = %s AND server_name = %s LIMIT 1", key)
    res = cur.fetchall()
    if res:
      infoid = res[0][0]
      print('Known server %s %d' % (key, infoid))
    else:
      cur.execute("INSERT INTO serverinfo (server_id, server_name) VALUES (%s, %s)", key)
      infoid = cur.lastrowid
      print('New server %s %d' % (key, infoid))
  cache_on_commit(cur, serverinfo_cache, key, infoid)
  if (sid not in serverinfo_current) or (serverinfo_current[sid] != infoid):
    print('Updating current serverinfo %d->%d' % (sid, infoid))
    cur.execute("REPLACE INTO serverinfo_current(server_id, serverinfo_id) VALUES (%s, %s)", (sid, infoid))
    cache_on_commit(cur, serverinfo_current, sid, infoid)
  return infoid

bots_logged = set()
@inqueue(logqueue)
@with_cursor
def log_chat(cur, message, si, sn, ci, cn, ui, un, message_text, is_bot):
  chanid = log_channel(cur, message.channel)
  userid = log_user(cur, message.author)
  serverid = log_server(cur, message.guild)
  for ch in message.channel_mentions:
    log_channel(cur, ch)
  cur.execute("INSERT INTO `chat` (`server_id`, `serverinfo_id`, `channel_id`, `channelinfo_id`, `user_id`, `userinfo_id`, `message`) VALUES (%s, %s, %s, %s, %s, %s, %s)", (si, serverid, ci, chanid, ui, userid, message_text))
  if is_bot and ui not in bots_logged:
    cur.execute("INSERT INTO `bots` (`id`) VALUES (%s) ON DUPLICATE KEY UPDATE id=id", (ui,))
    bots_logged.add(ui)

@inqueue(logqueue)
@cached(LRUCache(8*1024))
@with_cursor
def log_mention(cur, uid, name, mention):
  cur.execute("INSERT INTO `mentions` (`user_id`, `name`, `mention`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE counter = counter + 1", (uid, name, mention))

@inqueue(logqueue)
@cached(LRUCache(8*1024))
@with_cursor
def log_role(cur, server_id, role_id, role_name, role_mention):
  cur.execute("INSERT INTO `roles` (`server_id`, `role_id`, `role_name`, `role_mention`) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE counter = counter + 1", (server_id, role_id, role_name, role_mention))

badwordcache = LRUCache(8*1024)

@cached(badwordcache)
@with_cursor
def badword_get(cur, server_id):
  print('getting badwords for %d' % server_id)
  cur.execute("SELECT badword FROM badwords WHERE server_id=%s", (server_id,))
  return [x[0] for x in cur]

@with_cursor
def badword_add(cur, server_id, badword, addedby):
  cur.execute("INSERT INTO badwords (server_id, badword, addedby) VALUES (%s, %s, %s)", (server_id, badword, addedby))
  badwordcache.pop(hashkey(server_id), None)

@with_cursor
def badword_del(cur, server_id, badword):
  cur.execute("DELETE FROM badwords WHERE server_id = %s AND badword = %s", (server_id, badword))
  badwordcache.pop(hashkey(server_id), None)

client = discord.Client()

@client.event
async def on_ready():
  print('Logged in as')
  print(client.user.name)
  print(client.user.id)
  print('------')
  print('Trying to change presence')
  await client.change_presence(activity=discord.Game(name='@ me name or /!help'))
  print('Done')

def serveridname(server):
  if server:
    return (server.id, server.name)
  else:
    return (None, None)

def channelidname(channel):
  if channel:
    return (channel.id, channel.name if hasattr(channel,'name') else None)
  else:
    return (None, None)

def can_send(server, channel):
  if (not server) or (not channel):
    return True
  member = server.get_member(client.user.id)
  return channel.permissions_for(member).send_messages

def should_reply(si, sn, ci, cn, ui, un, txt, server, channel, author):
  opt_mention_only = options.get_option(si, ci, 'mention_only')
  opt_extra_prefix = options.get_option(si, ci, 'extra_prefix')

  keywords = ["<@%s>" % client.user.id, "<@!%s>" % client.user.id]

  # ignore empty messages
  if not txt:
    return (False, txt)

  # never reply to own messages
  if ui == client.user.id:
    return (False, txt)

  # ignore bots by default
  if author.bot and options.get_option(si, ci, 'reply_to_bots') == 0:
    return (False, txt)

  member = None
  if server:
    member = server.get_member(client.user.id)

  # check send perms
  if channel and member and (not channel.permissions_for(member).send_messages):
    return (False, txt)

  if opt_extra_prefix != "" and txt.lower().startswith(opt_extra_prefix.lower()):
    return (True, txt[len(opt_extra_prefix):])

  if opt_mention_only <= 0:
    keywords.append(Config.get('Chat', 'Keyword').lower())
    if member and member.nick:
      keywords.append(member.nick.lower())

#  print("kw: ", keywords)
  if options.get_option(si, ci, 'prefix_only') <= 0:
    for kw in keywords:
      if kw in txt.lower():
        return (True, txt)
  else:
    for kw in keywords:
      if txt.lower().startswith(kw):
        return (True, txt)

  prob = options.get_option(si, ci, 'reply_prob')
  if (uniform(0, 1) < prob):
    return (True, txt)
  return (False, txt)

help_links="""[Add me to your server](https://discordapp.com/oauth2/authorize?client_id=477996444775743488&scope=bot)
[Support server](https://discord.gg/EhNr4hR)
[DBL link](https://discordbots.org/bot/477996444775743488)"""

def make_help():
  emb = discord.Embed(description="Sobert's silly help thing")
  emb.add_field(name="/!help", value="Show this text")
  emb.add_field(name="/!set reply_prob *P*", value="Set my reply probability for the current channel to **P** (0 to 1.0). Defaults to 0, except in DMs.")
  emb.add_field(name="/!set max_bot_msg_length *L*", value="Don't process messages from bots longer than **L** characters. Defaults to 200.")
  emb.add_field(name="/!set prefix_only *0|1*", value="Only match keywords as prefixes, not anywhere in the message.")
  emb.add_field(name="/!set mention_only *0|1*", value="Don't match on name, only @mention.")
  emb.add_field(name="/!set extra_prefix *P*", value="Set an additional prefix to reply to")
  emb.add_field(name="/!set *option_name*", value="Unsets a previously set option")
  emb.add_field(name="/!badword *word*", value="Add or remove word to/from per-server bad word list")
  emb.add_field(name="Links and stuff", value=help_links)
  return emb

async def send_option_list(si, ci, channel):
  emb = discord.Embed(description="Options, set using /!set")
  for opt in options.options.values():
    if opt.settable:
      v = 'type: ' + opt.type.__name__
      if opt.default_group == opt.default_user:
        v += '\ndefault: %s' % opt.default_group
      else:
        v += '\nuser default: %s\nchannel default: %s' % (opt.default_user, opt.default_group)
      v += '\ncurrent value: %s' % options.get_option(si, ci, opt.name)
      v += '\n%s' % opt.description
      emb.add_field(name=opt.name, value=v)
  await channel.send(embed=emb)

#Type */!set reply_to_bots 0|1* to enable or disable. Defaults to 0.

cmd_replies = set()

currently_sending = {}

@client.event
async def on_message(message):
  start_time = time()
  msgcolor = ''
  ci, cn = channelidname(message.channel)
  si, sn = serveridname(message.guild)
  ui = message.author.id
  un = message.author.name
  txt = message.content

  if txt == "":
    return

  if options.get_option(si, ci, 'blacklisted') > 0:
    return

  cansend = can_send(message.guild, message.channel)

  channel_ignored = False
  if message.author.bot:
    msgcolor = '\033[34m'
  if options.get_option(si, ci, 'ignore_channel') > 0:
    channel_ignored = True
    msgcolor = '\033[90m'
  if not cansend:
    msgcolor = '\033[31m'
  if not message.guild:
    msgcolor = '\033[96m'
  if ui == client.user.id:
    msgcolor = '\033[92m'

  if not channel_ignored:
    print(msgcolor + ('%s/%s %s/%s %s/%s : %s' % (sn, si, cn, ci, message.author.name, message.author.id, txt)) + '\033[0m')

  if message.id in cmd_replies:
    print('(not logging)')
    return
  log_chat(message, si, sn, ci, cn, ui, un, txt, message.author.bot)
  for u in message.mentions:
    log_mention(u.id, u.name, u.mention)
  for r in message.role_mentions:
    log_role(si, r.id, r.name, r.mention)

  if not cansend:
    return

  if ui == client.user.id:
    return

  if channel_ignored == True:
    return

  if txt.startswith('/!') and message.author.bot:
    return

  if txt.startswith('/!help'):
    await message.channel.send(embed=make_help())

  elif txt.startswith('/!set '):
    if message.guild and not message.author.permissions_in(message.channel).manage_channels:
      cmd_replies.add((await message.channel.send("< only people with manage_channels permission can set options >")).id)
      return
    splt = txt.split()
    if (len(splt) == 3):
      opt = splt[1]
      val = splt[2]
      try:
        options.set_option(ci, opt, val)
        await message.channel.send("< option %s set to %s >" % (opt, val))
      except options.OptionError as oe:
        await message.channel.send("< %s >" % str(oe))
    elif (len(splt) == 2):
      opt = splt[1]
      try:
        options.set_option(ci, opt, None)
        await message.channel.send("< option %s unset >" % (opt,))
      except options.OptionError as oe:
        await message.channel.send("< %s >" % str(oe))
    else:
      cmd_replies.add((await message.channel.send("< invalid syntax, use /!set option value >")).id)
      return

  elif txt.startswith('/!clear'):
    options.optioncache.clear()
    print('options cache flushed')

  elif txt.startswith('/!badword '):
    if message.guild and not message.author.permissions_in(message.channel).manage_channels:
      await message.channel.send("< only people with manage_channels permission can change badwords >")
      return
    splt = txt.split(' ', 1)
    bw = splt[1]
    bws = badword_get(si or ci)
    if bw in bws:
      badword_del(si or ci, bw)
      await message.channel.send("< badword %s deleted >" % (bw))
    else:
      badword_add(si or ci, bw, ui)
      await message.channel.send("< badword %s added >" % (bw))

  elif txt == '/!badword':
    bws = badword_get(si or ci)
    await message.channel.send("< badwords: %s >" % repr(bws))

  elif txt == '/!list_options':
    await send_option_list(si, ci, message.channel)

  else:
    queued = await nn.queued_for_key(str(ci))
    if queued > 32:
      print('Dropping message, %d messages queued' % queued)
      return

    (shld_reply, new_text) = should_reply(si, sn, ci, cn, ui, un, txt, message.guild, message.channel, message.author)

    if (not message.author.bot) or (len(txt) <= options.get_option(si, ci, 'max_bot_msg_length')):
      txt2 = new_text
      for u in message.mentions:
        txt2 = txt2.replace(u.mention, '@'+u.name)
        txt2 = txt2.replace('<@!%d>'%u.id, '@'+u.name)
      for r in message.role_mentions:
        txt2 = txt2.replace(r.mention, '@'+r.name)
      for c in message.channel_mentions:
        txt2 = txt2.replace(c.mention, '#'+c.name)
      if txt2 != new_text:
        print(" interpreted as %s" % txt2)
      await nn.put(str(ci), txt2)

    
    if shld_reply:
      if ci not in currently_sending:
        currently_sending[ci] = 0
      if currently_sending[ci] > 8:
        print('%d messages being sent, not responding' % currently_sending[ci])
        return
      currently_sending[ci] += 1
      try:
        async with message.channel.typing():
          rpl_txt = await nn.get(str(ci), bad_words = badword_get(si or ci))
          if rpl_txt == '':
            print('ignoring empty reply')
            return
          rpl_msg = await message.channel.send(rpl_txt)
      finally:
        currently_sending[ci] -= 1
      end_time = time()
      reply_delay = end_time - start_time
      if reply_delay > 20:
        await rpl_msg.edit(content=rpl_txt + ('\n*reply delayed by %f seconds*' % (reply_delay)))
        print('message took %f seconds to generate' % (reply_delay))

client.run(Config.get('Discord', 'Token'))
