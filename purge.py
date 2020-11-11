import sys
from sobutils.configuration import Config
from sobutils.database import with_cursor, cache_on_commit

Config.read(sys.argv[1])

def get_batch(cur, lastid, cnt):
  cur.execute("SELECT id, "
              "  UNHEX(SHA2(message,256)) IN (SELECT hash FROM bad_messages) AS msg_bad, "
              "  user_id IN (SELECT id FROM bots) AND user_id NOT IN (SELECT id FROM good_bots) AS msg_bot, "
              "  count, message "
              "FROM chat "
              "LEFT JOIN chat_hashcounts ON UNHEX(SHA2(message,256)) = hash "
              "WHERE id > %s "
              "ORDER BY id "
              "ASC LIMIT %s", (lastid, cnt))
  return cur.fetchall()

@with_cursor
def scan_db(cur):
  lastid = -1
  while True:
    batch = get_batch(cur, lastid, 1000)
    if not batch:
      print("No messages, done")
      break
    print("Got %d messages ids <%d,%d>" % (len(batch), batch[0][0], batch[-1][0]))
    lastid = batch[-1][0]
    for messagerow in batch:
      (mid, mbad, mbot, mcount, mtext) = messagerow
      if mbad and mbot:
        print('Message %d is bot and bad! count=%d (%s)' % (mid, mcount, mtext))

scan_db()
