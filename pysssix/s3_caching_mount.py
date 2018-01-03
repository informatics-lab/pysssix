from .s3 import open as s3_open, getattr as s3_getattr, list_bucket # TODO: relative imports
from fuse import Operations
import logging
import random 

# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)



class S3FileSystemMount(Operations):        

    def __init__(self):
        self.openfh = {};

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
        self.openfh[fh] = s3_open(file)
        return fh

    def read(self, path, size, offset, fh):
        logger.info("read: %s (%s)" % (path, fh))
        s3_reader = self.openfh[fh] # TODO: not sure we need to use a reader to maintain the seek possition, offset might handel it for us...
        s3_reader.seek(offset)
        data = s3_reader.read(size, offset)
        logger.info("read: len %s" % len(data))
        return b''.join((chunk.read() for chunk in data))

    def release(self, path, fh):
        logger.info("release: %s (%s)" % (path, fh))
        del self.openfh[fh]
        return

    def readdir(self, path, fh):
        logger.info("readdir: %s", path)
        return list_bucket(path)

