import sys
from collections import namedtuple

from sobutils.configuration import Config
from sobutils.database import get_dbcon, with_cursor

Config.read(sys.argv[1])

MsgRow = namedtuple('MsgRow', 'id is_bad is_bot age ch_bad ch_black count first_id user_id channel_id server_id expire_days channel_expire text')

def get_maxid(cur):
  cur.execute("SELECT MAX(id) FROM chat")
  return cur.fetchone()[0]

def get_batch(cur, lastid, cnt):
  cur.execute("SELECT id, "
              "  UNHEX(SHA2(message,256)) IN (SELECT hash FROM bad_messages) AS msg_bad, "
              "  user_id IN (SELECT id FROM bots) AND user_id NOT IN (SELECT id FROM good_bots) AS msg_bot, "
              "  TIMESTAMPDIFF(DAY, date, CURRENT_TIMESTAMP) AS age, "
              "  is_bad, blacklisted, count, message_id, user_id, channel_id, server_id, expire_days, delete_after, message "
              "FROM chat "
              "LEFT JOIN chat_hashcounts ON UNHEX(SHA2(message,256)) = hash "
              "LEFT JOIN options2 USING (channel_id) "
              "LEFT JOIN server_expire USING (server_id) "
              "WHERE id > %s "
              "ORDER BY id "
              "ASC LIMIT %s", (lastid, cnt))
  return cur.fetchall()

def should_delete(msg):
  expire_age = 30
  if msg.expire_days is not None:
    expire_age = msg.expire_days
  if msg.channel_expire is not None:
    expire_age = msg.channel_expire

  if msg.ch_bad and msg.ch_black and msg.age > 7: # purge blacklisted channels faster
    return True

  if msg.age < expire_age: # don't delete messages that aren't old enough
    return False

  return True

def try_delete(cur, msg):
  #print('Trying to delete: %s' % (msg,))
  if msg.count is None:
    print('Bad message? %s' % (msg,))
  if msg.count > 1 and msg.first_id != msg.id:
    print('Deleting: %s' % (msg,))
    cur.execute("UPDATE chat_hashcounts SET count = count - 1 WHERE hash=UNHEX(SHA2((SELECT message FROM chat WHERE id=%s),256)) AND message_id <> %s", (msg.id, msg.id))
    assert(cur.rowcount==1)
    cur.execute("UPDATE chat_counters SET message_count = message_count - 1 WHERE channel_id = %s AND user_id = %s", (msg.channel_id, msg.user_id))
    assert(cur.rowcount<=1)
    cur.execute("DELETE FROM chat WHERE id=%s", (msg.id,))
    assert(cur.rowcount==1)
    if msg.is_bot:
      cur.execute("INSERT INTO deleted_counter (channel_id, del_bot_r) VALUES (%s,1) ON DUPLICATE KEY UPDATE del_bot_r = del_bot_r + 1", (msg.channel_id,))
    else:
      cur.execute("INSERT INTO deleted_counter (channel_id, del_user_r) VALUES (%s,1) ON DUPLICATE KEY UPDATE del_user_r = del_user_r + 1", (msg.channel_id,))
      cur.execute("UPDATE chat_uniqueness SET last_count = last_count -1 WHERE channel_id=%s", (msg.channel_id,))
      assert(cur.rowcount==1 or cur.rowcount == 0)
    return True
  if msg.count == 1:
    print('Deleting: %s' % (msg,))
    assert(msg.first_id == msg.id)
    cur.execute("DELETE FROM chat_hashcounts WHERE hash=UNHEX(SHA2((SELECT message FROM chat WHERE id=%s),256)) AND count=1", (msg.id,))
    assert(cur.rowcount==1)
    cur.execute("UPDATE chat_counters SET message_count = message_count - 1 WHERE channel_id = %s AND user_id = %s", (msg.channel_id, msg.user_id))
    if (cur.rowcount != 1):
      print("Warning: no counter?")
    assert(cur.rowcount<=1)
    cur.execute("DELETE FROM chat WHERE id=%s", (msg.id,))
    assert(cur.rowcount==1)
    if msg.is_bot:
      cur.execute("INSERT INTO deleted_counter (channel_id, del_bot_u) VALUES (%s,1) ON DUPLICATE KEY UPDATE del_bot_u = del_bot_u + 1", (msg.channel_id,))
    else:
      cur.execute("INSERT INTO deleted_counter (channel_id, del_user_u) VALUES (%s,1) ON DUPLICATE KEY UPDATE del_user_u = del_user_u + 1", (msg.channel_id,))
      cur.execute("UPDATE chat_uniqueness SET last_count = last_count -1 WHERE channel_id=%s", (msg.channel_id,))
      assert(cur.rowcount==1 or cur.rowcount == 0)
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
    print("Checked %d (%.2f%%) deletable %d (%.2f%%) deleted %d (%.2f%%) undeletable %d" % (cnt_checked, cnt_checked / lastid * 100, cnt_deletable, cnt_deletable/cnt_checked*100, cnt_deleted, cnt_deleted/cnt_checked*100, cnt_deletable - cnt_deleted))
    dbcon.commit()
    if cnt_deleted > 250000:
      break

@with_cursor
def cleanup_counters(cur):
  cur.execute("DELETE FROM chat_counters WHERE message_count=0");
  print("Deleted %d zero counters" % cur.rowcount)

scan_db()
cleanup_counters()
