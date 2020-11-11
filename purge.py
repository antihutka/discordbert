import sys
from sobutils.configuration import Config
from sobutils.database import with_cursor, cache_on_commit

Config.read(sys.argv[1])

def get_batch(cur, lastid, cnt):
  cur.execute("SELECT id FROM chat WHERE id > %s ORDER BY id ASC LIMIT %s", (lastid, cnt))
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

scan_db()
