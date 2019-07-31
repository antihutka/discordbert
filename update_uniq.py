import time

from botlib.util import aretry
from botlib.configuration import Config, read_config
from botlib.db import with_cursor

read_config()

@with_cursor
def add_new_chats(cur):
  cur.execute("SELECT channel_id FROM chat_counters "
              "  WHERE channel_id NOT IN (SELECT channel_id FROM chat_uniqueness) AND user_id NOT IN (SELECT id FROM bots) "
              "  GROUP BY channel_id "
              "  HAVING SUM(message_count) > 100")
  vals = cur.fetchall()
  cur.executemany("INSERT INTO chat_uniqueness(channel_id) VALUES (%s)", vals)
  if cur.rowcount > 0:
    print("Added %d new chats" % cur.rowcount)



get_chats_q = """
SELECT * FROM (
  SELECT channel_id,
         message_count,
         (message_count - last_count) as new_messages,
         age,
         CAST((100 * (message_count - last_count))/(100+message_count) + age / (1440 * 7)  AS DOUBLE) AS score,
         COALESCE(uniqueness, -1) AS uniqueness,
         (SELECT COALESCE(CONCAT(server_name, "/", channel_name), user_name) FROM chat WHERE chat.channel_id = a.channel_id AND user_id NOT IN (SELECT id FROM bots) ORDER BY id DESC LIMIT 1) as chatname
  FROM (
    SELECT channel_id,
           message_count,
           last_count,
           TIMESTAMPDIFF(MINUTE, last_update, CURRENT_TIMESTAMP) AS age,
           uniqueness
    FROM chat_uniqueness LEFT JOIN channel_counts_nobots USING (channel_id)
  ) a
) b WHERE score > 0.1 OR uniqueness < 0 ORDER BY score DESC LIMIT 10;
"""

def get_scores(cur):
  cur.execute(get_chats_q)
  return cur.fetchall()

def get_server_for_channel(cur, channel_id):
  cur.execute("SELECT server_id FROM chat_counters WHERE channel_id=%s LIMIT 1", (channel_id,))
  return cur.fetchone()[0]

def get_score(cur, server_id, channel_id):
  cur.execute("SELECT COALESCE(SUM(IF(count=1, 1, 0)) / COUNT(*), 0) "
              "  FROM chat LEFT JOIN chat_hashcounts ON hash=UNHEX(SHA2(message, 256)) "
              "  WHERE user_id NOT IN (SELECT id FROM bots) "
              "    AND chat.server_id <=> %s AND chat.channel_id=%s", (server_id,channel_id))
  return cur.fetchone()[0]

def write_score(cur, channel_id, uniq, cnt):
  cur.execute("UPDATE chat_uniqueness SET "
              "  uniqueness = %s, "
              "  last_count = %s, "
              "  last_update = CURRENT_TIMESTAMP "
              "WHERE channel_id = %s", (uniq, cnt, channel_id))

@with_cursor
def update_step(cur):
  chats_to_update = get_scores(cur)
  if not chats_to_update:
    print("No chats to update")
    return 0
  for i in chats_to_update:
    print("Chat: %16d New: %6d / %6d updated: %6d minutes ago score: %4.2f uniq: %.3f %s" % i)
  (channel_id, msg_count, msg_new, age, score, uniq, chatname) = chats_to_update[0]
  server_id = get_server_for_channel(cur, channel_id)
  print("Updating stats for %s %d %s" % (server_id, channel_id, chatname))
  new_uniq = get_score(cur, server_id, channel_id)
  print("Changed uniq from %f to %f (%f)" % (uniq, new_uniq, float(new_uniq)-float(uniq)))
  write_score(cur, channel_id, new_uniq, msg_count)
  return score

varsleep = 300

while True:
  starttime = time.time()
  add_new_chats()
  score = update_step()
  endtime = time.time()
  elaps = endtime-starttime
  if score < 0.9:
    varsleep = varsleep + 1
  if score > 1.1 and varsleep > 10:
    varsleep = varsleep - 1
  sleeptime = (elaps * 10 + varsleep) / max(0.25, score)
  print("Took %f, sleep for %f" % (elaps, sleeptime))
  time.sleep(sleeptime)
