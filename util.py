import asyncio
import logging
import random

def aretry(retry_count):
  def dec(f):
    async def df(*args, **kwargs):
      fails = 0
      delay = 0
      while True:
        try:
          return await f(*args, **kwargs)
        except:
          fails += 1
          if retry_count >= fails:
            logging.exception('Exception, retrying in %d seconds' % delay)
            await asyncio.sleep(delay)
            delay = delay * 2 + random.randint(1,2)
          else:
            logging.exception('Exception, giving up')
            raise
    return df
  return dec

