from .s3 import getattr as s3_getattr, list_bucket, get_bytes # TODO: relative imports
from fuse import Operations
import logging
import random 
from .block_cache import BlockCache

# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)


class NoCache(object):
    def __init__(self, requester):
        self.requester = requester
        
    def get(self, key, offset, size):
        return self.requester(key, offset, size-1)

class S3FUSEOps(Operations):        

    def __init__(self, cache_size=None, cache_path=None, block_size=None):
        if(cache_size > 0):
            logger.info('Using BlockCache')
            self.cache = BlockCache(get_bytes, cache_size=cache_size, block_size=block_size, cache_path=cache_path)
        else:
            logger.info('Using NoCache')
            self.cache = NoCache(get_bytes)

    def flush(self, path, fh):
        return None

    def getattr(self, path, fh=None):
        logger.info("getattr: %s" % path)
        attrs =  s3_getattr(path)
        logger.info("getattr: found %s for %s" % (attrs, path))
        return attrs


    def open(self, file, flags, mode=None):
        logger.info("open: %s", file)
        fh = random.randint(5, 2147483646)
        return fh

    def read(self, path, size, offset, fh):
        logger.info("read: %s (%s). Size: %s, offset: %s" % (path, fh, size, offset))
        data = self.cache.get(path, offset , size)
        logger.info("read: len %s" % len(data))
        return data

    def release(self, path, fh):
        logger.info("release: %s (%s)" % (path, fh))
        return

    def readdir(self, path, fh):
        logger.info("readdir: %s", path)
        return list_bucket(path)