import math
import asyncio
from diskcache import FanoutCache
from functools import partial
from operator import itemgetter
import os.path
import logging

logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)

class BlockCache(object):
    
    def __init__(self, requester, block_size=None, cache_path=None, cache_size=None):
        
        block_size = block_size if block_size else 2048
        cache_path = cache_path if cache_path else '~/.pyssssix_cache'
        cache_size = cache_size if cache_size else 4e9

        logger.info("Cache params block_size %s, cache_path %s, cache_size %s" % (
                    block_size, cache_path, cache_size))

        # TODO: configure more?
        cache_path = os.path.abspath(os.path.expanduser(cache_path))
        logger.info('cache at %s' % cache_path)
        self.cache = FanoutCache(cache_path, shards=8, size_limit=int(cache_size), timeout=0.5)
        self.requester = requester
        self.blocksize = block_size
    
    def __del__(self):
        logger.info("__del__ BlockCache. Close cache.")
        self.cache.close()

    @asyncio.coroutine
    def get_and_save(self, key, record):
        start = record['block'] * self.blocksize
        stop = start + self.blocksize -1
        data = yield from (asyncio.get_event_loop().run_in_executor(None, self.requester, key, start, stop))
        self.cache.set(record['ckey'], data)
        return {
            "ckey":record['ckey'],
            "block":record['block'],
            "data":data
        }   

    def ckey(self, key, i):
        return '%d-%d--%s' % (i * self.blocksize, (i+1) * self.blocksize -1, key)

    def get(self, key, offset, size):
        logger.debug("block and get: %s off:%s size:%s" %(key, offset, size))
        blocks, start_at, last_chunk_size = self._which_blocks(offset, size)
        logger.debug("blocks = %s, start_at = %s, last_chunk_size = %s" % (blocks, start_at, last_chunk_size))

        hits, misses = self._get_hits_and_misses(key, blocks)

        logger.debug("block cache get(self, key=%s, offset=%s, size=%s)" % (key, offset, size))
        logger.debug("hits len %s, misses len %s." % (len(hits), len(misses)))


        loop = asyncio.new_event_loop()
        filled_holes_future = asyncio.gather(*(self.get_and_save(key, miss) for miss in misses),loop=loop)
        loop.run_until_complete(filled_holes_future)
        loop.close()
        filled_holes = filled_holes_future.result()

        # misses = db.from_sequence(misses).map(partial(self.get_and_save, key)).compute()
        # data = b''.join(record['data'] for record in sorted(list(misses) + hits, key=itemgetter('block')))
        
        
        chunks = list(record['data'] for record in sorted(filled_holes + hits, key=itemgetter('block')))
        

        logger.debug(',' .join("chunk %s len %s" %( i, len(chunk)) for i, chunk in enumerate(chunks)))
            
        data = b''.join(chunks)
        logger.debug("raw chunks data is len %s" % len(data))
        data = data[start_at:start_at+size]
        logger.debug("clipped chunk data is len %s" % len(data))  

        return data 
        
    def _get_hits_and_misses(self, key, blocks):
        hits, misses = [], []
        for block in blocks:
            ckey = self.ckey(key,block)
            record = {"ckey":ckey, "block":block}
            data = self.cache.get(ckey)
            if data is not None:
                record['data'] = data
                hits.append(record)
            else :
                misses.append(record)
                
        return hits, misses
    
    def _which_blocks(self, offset, size):
        first_block = math.floor(offset/self.blocksize)
        last_block = math.floor((offset+size)/self.blocksize)
        blocks = list(range(first_block, last_block +1))
        start_at = int(offset - first_block * self.blocksize)
        last_chunk_size = self.blocksize - (self.blocksize * len(blocks) - (start_at + size))
        return blocks, start_at, last_chunk_size
        


def _which_blocks( offset, size):
        first_block = math.floor(offset/4092)
        last_block = math.floor((offset+size)/4092)
        blocks = list(range(first_block, last_block +1))
        start_at = int(offset - first_block * 4092)
        last_chunk_size = 4092 - (4092 * len(blocks) - (start_at + size))
        return blocks, start_at, last_chunk_size