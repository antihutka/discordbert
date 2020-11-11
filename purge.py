import sys
from collections import namedtuple

from sobutils.configuration import Config
from sobutils.database import get_dbcon

Config.read(sys.argv[1])

MsgRow = namedtuple('MsgRow', 'id is_bad is_bot age ch_bad ch_black count first_id channel_id text')

def get_maxid(cur):
  cur.execute("SELECT MAX(id) FROM chat")
  return cur.fetchone()[0]

def get_batch(cur, lastid, cnt):
  cur.execute("SELECT id, "
              "  UNHEX(SHA2(message,256)) IN (SELECT hash FROM bad_messages) AS msg_bad, "
              "  user_id IN (SELECT id FROM bots) AND user_id NOT IN (SELECT id FROM good_bots) AS msg_bot, "
              "  TIMESTAMPDIFF(DAY, date, CURRENT_TIMESTAMP) AS age, "
              "  is_bad, blacklisted, count, message_id, channel_id, message "
              "FROM chat "
              "LEFT JOIN chat_hashcounts ON UNHEX(SHA2(message,256)) = hash "
              "LEFT JOIN options2 USING (channel_id) "
              "WHERE id > %s "
              "ORDER BY id "
              "ASC LIMIT %s", (lastid, cnt))
  return cur.fetchall()

def should_delete(msg):
  if msg.age < 30:
    return False
  if msg.is_bot and msg.is_bad and msg.ch_bad and msg.ch_black and len(msg.text) > 50:
    return True
  return False

def try_delete(cur, msg):
  print('Deleting: %s' % (msg,))
  if msg.count > 1 and msg.first_id < msg.id:
    cur.execute("UPDATE chat_hashcounts SET count = count - 1 WHERE hash=UNHEX(SHA2((SELECT message FROM chat WHERE id=%s),256))", (msg.id,))
    assert(cur.rowcount==1)
    cur.execute("DELETE FROM chat WHERE id=%s", (msg.id,))
    assert(cur.rowcount==1)
    if msg.is_bot:
      cur.execute("INSERT INTO deleted_counter (channel_id, del_bot_r) VALUES (%s,1) ON DUPLICATE KEY UPDATE del_bot_r = del_bot_r + 1", (msg.channel_id,))
    else:
      cur.execute("INSERT INTO deleted_counter (channel_id, del_user_r) VALUES (%s,1) ON DUPLICATE KEY UPDATE del_user_r = del_user_r + 1", (msg.channel_id,))
    return True
  return False

def scan_db():
  dbcon, cur = get_dbcon()
  lastid = -1
  cnt_checked = 0
  cnt_deletable = 0
  cnt_deleted = 0
  maxid = get_maxid(cur)
  while True:
    batch = get_batch(cur, lastid, 10000)
    if not batch:
      print("No messages, done")
      break
    cnt_checked += len(batch)
    lastid = batch[-1][0]
    print("Got %d messages ids <%d,%d> %d remaining" % (len(batch), batch[0][0], batch[-1][0], maxid - lastid))
    for messagerow in batch:
      msg = MsgRow(*messagerow)
      if should_delete(msg):
        cnt_deletable += 1
        if try_delete(cur, msg):
          cnt_deleted += 1
    print("Checked %d deletable %d deleted %d" % (cnt_checked, cnt_deletable, cnt_deleted))
    dbcon.commit()

scan_db()
