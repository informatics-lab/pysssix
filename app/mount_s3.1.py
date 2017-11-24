#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
import sys
import logging
from functools  import lru_cache
from sys import argv, exit
from fuse import FUSE, Operations, LoggingMixIn
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# logger.basicConfig(stream=sys.stdout, level=logging.DEBUG)

s3 = boto3.resource('s3')


"""
With thanks to @perrygeo(https://gist.github.com/perrygeo)
For the s3 file-like wrapper code. Now torn apart but inspired from:
https://gist.github.com/perrygeo/9239b9ab64731cacbb35#file-s3reader-py
"""

def open(path):
    return S3Reader(path)

@lru_cache(maxsize=32)
def get_s3_obj(orig_path):
    logger.info("Creating S3 object for %s", orig_path)
    path = 's3:/' + orig_path;
    bucket, key = parse_path(path)
    obj = s3.Object(bucket, key)
    obj.orig_path = orig_path
    return obj

def range_string(start, stop):
        return "bytes={}-{}".format(start, stop)

# TODO: worth caching? @lru_cache(maxsize=128)
def parse_path(path):
    if not path.startswith("s3://"):
        raise ValueError("s3reader.open requires an s3:// URI")
    path = path.replace("s3://", "")
    parts = path.split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])
    return bucket, key


def size_limited_caching_byte_request(path, start, stop):
    method = get_bytes if (stop - start < 17000) else  get_bytes.__wrapped__ # the limit should be just over a metadata/chunk request limit?
    return method(path, start, stop)

@lru_cache(maxsize=128)
def get_bytes(path, start, stop):
    rng=range_string(start, stop)
    logger.info("Request %s between %s", path, rng)
    return get_s3_obj(path).get(Range=rng)['Body'].read()



class S3Reader(object):
    def __init__(self, path):
        self.size = get_s3_obj(path).content_length
        self.pos = 0  # pointer to starting read position
        self.path = path

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.pos = 0

    def read(self, nbytes=None):
        if not nbytes:
            nbytes = self.size - self.pos
        # TODO confirm that start and stop bytes are within 0 to size
        the_bytes =  size_limited_caching_byte_request(self.path, self.pos , self.pos + nbytes - 1)
        self.pos += nbytes 
        return the_bytes

    def seek(self, offset, whence=0):
        self.pos = whence + offset






class S3FileSystemMount(Operations):        

    def __init__(self):
        self.count = 0;
        self.openfh = {};

    def flush(self, path, fh):
        return None

    def getattr(self, path, fh=None):
        """
        st_dev − ID of device containing file
        st_mode − protection
        st_nlink − number of hard links
        st_uid − user ID of owner
        st_gid − group ID of owner
        st_size − total size, in bytes
        st_blksize − blocksize for filesystem I/O
        st_blocks − number of blocks allocated
        st_atime − time of last access
        st_mtime − time of last modification
        st_ctime − time of last status change
        dir =  {'st_atime': 1511429888.0, 'st_ctime': 1511429894.0, 'st_gid': 0, 'st_mode': 16877, 'st_mtime': 1511429894.0, 'st_nlink': 3, 'st_size': 96, 'st_uid': 0}
        file = {'st_atime': 1511369446.0, 'st_ctime': 1511369374.0, 'st_gid': 0, 'st_mode': 33188, 'st_mtime': 1511367154.0, 'st_nlink': 1, 'st_size': 36047559, 'st_uid': 0}
        """
        if (path[-3:] == '.nc'):
            return {'st_mode': 33188, 'st_size': open(path).size}
        else :
            return {'st_mode': 16877}
        

    def open(self, file, flags, mode=None):
        self.count += 1
        fh = self.count
        self.openfh[fh] = open(file)
        return fh

    def read(self, path, size, offset, fh):
        self.openfh[fh].seek(offset)
        return self.openfh[fh].read(size)

    def release(self, path, fh):
        del self.openfh[fh]
        return


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    #logging.basicConfig(level=logging.DEBUG)

    # logging.getLogger("botocore").setLevel(logging.WARNING)
    # logging.getLogger("fuse.log-mixin").setLevel(logging.WARNING)

    

    logger.info("Starting up s3 fuse at mount point %s", argv[1])
    fuse = FUSE(S3FileSystemMount(),argv[1], foreground=True)