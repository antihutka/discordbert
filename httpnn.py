import aiohttp
import asyncio
from util import aretry

class HTTPNN:
  def __init__(self, url, keyprefix):
    self.url = url
    self.keyprefix = keyprefix
    self.locks = {}

  def get_lock(self, key):
    if key not in self.locks:
      self.locks[key] = asyncio.Lock()
    return self.locks[key]

  async def queued_for_key(self, key):
    w = self.get_lock(key)._waiters
    if w is None:
      return 0
    return len(w)

  @aretry(5)
  async def put_(self, key, message):
    async with self.client.post(self.url + "put", json={'key': self.keyprefix + ':' + key, 'text': message}) as response:
      assert response.status == 200
      rj = await response.json()

  async def put(self, key, message):
    async with self.get_lock(key):
      return await self.put_(key, message)

  @aretry(5)
  async def get_(self, key, bad_words):
    async with self.client.post(self.url + 'get', json={'key': self.keyprefix + ':' + key, 'bad_words': bad_words}) as response:
      assert response.status == 200
      rj = await response.json()
    return rj['text']

  async def get(self, key, bad_words = []):
    async with self.get_lock(key):
      return await self.get_(key, bad_words)

  async def initialize(self):
    self.client = aiohttp.ClientSession(loop = asyncio.get_event_loop(), timeout = aiohttp.ClientTimeout(900))
