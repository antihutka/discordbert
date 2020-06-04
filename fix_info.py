import sys
import time

from sobutils.configuration import Config
from sobutils.database import with_cursor, cache_on_commit

Config.read(sys.argv[1])

channelinfo_cache = {}
def log_channel(cur, cid, cname, cserver):
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
  return infoid

userinfo_cache = {}
def log_user(cur, uid, uname, ubot):
  unick = None
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
  return infoid

serverinfo_cache = {}
def log_server(cur, sid, sname):
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
  return infoid

batch_max = 5000

@with_cursor
def fix_server(cur, minid):
  cur.execute("SELECT id, server_id, server_name FROM chat WHERE server_id IS NOT NULL AND serverinfo_id IS NULL AND id > %s ORDER BY id ASC LIMIT 1", (minid,))
  r = cur.fetchone()
  if not r:
    print('No server to fix')
    return None
  (msgid, servid, servname) = r
  print('Found server %s id %d msg %d' % (servname, servid, msgid))
  siid = log_server(cur, servid, servname)
  print('Info id %d' % siid)
  cur.execute("UPDATE chat SET serverinfo_id=%s WHERE serverinfo_id IS NULL AND server_name=%s AND server_id=%s AND id > %s AND id < %s", (siid, servname, servid, msgid-1, msgid + batch_max))
  print('Affected rows: %d' % cur.rowcount)
  return msgid

@with_cursor
def fix_user(cur, minid):
  cur.execute("SELECT id, user_id, user_name, user_id IN (SELECT id FROM bots), server_id FROM chat WHERE user_id IS NOT NULL AND userinfo_id IS NULL AND id > %s ORDER BY id ASC LIMIT 1", (minid,))
  r = cur.fetchone()
  if not r:
    print('No user to fix')
    return None
  (msgid, userid, username, isbot, servid) = r
  print('Found user %s id %d msg %d' % (username, userid, msgid))
  uiid = log_user(cur, userid, username, isbot)
  print('Info id %d' % uiid)
  cur.execute("UPDATE chat SET userinfo_id=%s WHERE userinfo_id IS NULL AND user_name=%s AND user_id=%s AND id > %s AND id < %s AND server_id <=> %s", (uiid, username, userid, msgid-1, msgid + batch_max, servid))
  print('Affected rows: %d' % cur.rowcount)
  return msgid

@with_cursor
def fix_channel(cur, minid):
  cur.execute("SELECT id, channel_id, channel_name, server_id FROM chat WHERE channel_id IS NOT NULL AND channelinfo_id IS NULL AND id > %s ORDER BY id ASC LIMIT 1", (minid,))
  r = cur.fetchone()
  if not r:
    print('No channel to fix')
    return None
  (msgid, chanid, channame, servid) = r
  print('Found channel %s id %d serv %s msg %d' % (channame, chanid, repr(servid), msgid))
  ciid = log_channel(cur, chanid, channame, servid)
  print('Info id %d' % ciid)
  cur.execute("UPDATE chat SET channelinfo_id=%s WHERE channelinfo_id IS NULL AND channel_name<=>%s AND channel_id=%s AND server_id<=>%s AND id > %s AND id < %s", (ciid, channame, chanid, servid, msgid-1, msgid + batch_max))
  print('Affected rows: %d' % cur.rowcount)
  return msgid


for fixfun in [fix_server, fix_user, fix_channel]:
  lmsg = -1
  while True:
    lmsg = fixfun(lmsg)
    time.sleep(0.8)
    if lmsg is None:
      break
