#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
import logging
from functools  import lru_cache
from fuse import FUSE, Operations, FuseOSError
import boto3
import os.path
from errno import ENOENT
from botocore.exceptions import ClientError

# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)

"""
With thanks to @perrygeo(https://gist.github.com/perrygeo)
For the s3 file-like wrapper code. Now torn apart but inspired from:
https://gist.github.com/perrygeo/9239b9ab64731cacbb35#file-s3reader-py
"""

s3 = boto3.resource('s3')

def open(path):
    return S3Reader(path)

@lru_cache(maxsize=1024)
def get_s3_obj(path):
    logger.info("Creating S3 object for %s", path)
    bucket, key = parse_path(path)
    obj = s3.Object(bucket, key)
    obj.path = path
    return obj

def range_string(start, stop):
        return "bytes={}-{}".format(start, stop)

# TODO: worth caching? @lru_cache(maxsize=128)
def parse_path(path):
    path = path[1:] if path[0] == '/' else path 
    parts = path.split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])
    return bucket, key

def size_limited_caching_byte_request(path, start, stop):
    method = get_bytes if (stop - start < 2e+7) else  get_bytes.__wrapped__ # the limit should be just over a metadata/chunk request limit?
    return method(path, start, stop)

@lru_cache(maxsize=4096)
def get_bytes(path, start, stop):
    rng=range_string(start, stop)
    logger.info("Request %s between %s", path, rng)
    return get_s3_obj(path).get(Range=rng)['Body'].read()


@lru_cache(maxsize=1024)
def obj_type(path):
    """
    0 not found
    1 dir
    2 file
    """

    # Test if any object in bucket has prefix
    try:    
        bucket, key = parse_path(path)
        if not len(key) > 0:
            return 1
        boto3.client('s3').list_objects_v2(Bucket=bucket,Prefix=key,MaxKeys=1)['Contents']
    except KeyError:
        raise FuseOSError(ENOENT)

    # Test if path represents a complete bucket, key pair.
    try:
        if get_s3_obj(path).content_length <= 0:
            raise ValueError("Content empty")
        return 2 # Object exists. It's a file.
    except ValueError as e:
        raise FuseOSError(ENOENT)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return 1 # The key doesn't exist so treat as a directory
        else:
            raise # Something else has gone wrong.

@lru_cache(maxsize=1024)
def list_bucket(path):
    logger.info("Requested ls for %s", path)
    bucket, key = parse_path(path)
    logger.info("Requested ls for bucket %s , key %s", bucket, key)

    if not bucket:
        return ['.', '..']
    
    try:
        def parse(entry):
            prefix = key[:-1] if key and key[-1] == '/' else key
            s3_key = entry['Key']
            after_fix = s3_key[len(prefix):]
            if(after_fix[0] == '/'):
                # show next level
                return after_fix.split('/')[1]
            else :
                # finish this level
                # TODO: bug if key ends with '/' but who would do that!?
                return prefix.split('/')[-1] + after_fix.split('/')[0]

        items = boto3.client('s3').list_objects_v2(Bucket=bucket,Prefix=key)['Contents']
        items = map(parse, items)
        items = [i for i in set(items) if i]
    except KeyError:
        items = []

    logger.info("Found %s for %s", items, path)

    return ['.', '..'] + items

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
        logger.info("you asked for %s", path)
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

        attr =   {'st_mode': 33188, 'st_size': open(path).size} if obj_type(path) == 2 else {'st_mode': 16877}
        logger.info("found %s" % attr)
        return attr

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

    def readdir(self, path, fh):
        return list_bucket(path)



from .s3 import open as s3_open, getattr as s3_getattr, list_bucket # TODO: relative imports
import random 




class S3Handeler(Operations):   


    def __init__(self):
        self.count = 0;
        self.openfh = {};
        #self.openfh = {};

    def flush(self, path, fh):
        return None

    def getattr(self, path, fh=None):
        # logger.info("you asked for %s", path)
        # attr =   {'st_mode': 33188, 'st_size': open(path).size} if obj_type(path) == 2 else {'st_mode': 16877}
        # logger.info("found %s" % attr)
        # return attr

        logger.info("getattr: %s" % path)
        attrs =  s3_getattr(path)
        logger.info("getattr: found %s for %s" % (attrs, path))
        return attrs

    def open(self, file, flags, mode=None):
        # self.count += 1
        # fh = self.count
        # self.openfh[fh] = open(file)
        # return fh


        logger.info("open: %s", file)
        fh = random.randint(5, 2147483646)
        self.openfh[fh] = s3_open(file)
        return fh


    def read(self, path, size, offset, fh):
        logger.info("read: %s (%s)" % (path, fh))
        # self.openfh[fh].seek(offset)
        # return self.openfh[fh].read(size)
        logger.info("read: %s (%s)" % (path, fh))
        s3_reader = self.openfh[fh] # TODO: not sure we need to use a reader to maintain the seek possition, offset might handel it for us...
        s3_reader.seek(offset)
        data = s3_reader.read(size, offset)
        logger.info("read: len %s" % len(data))        
        return b''.join((chunk.read() for chunk in data))
        
    def release(self, path, fh):
        del self.openfh[fh]
        return

        # logger.info("release: %s (%s)" % (path, fh))
        # del self.openfh[fh]
        # return

    def readdir(self, path, fh):
        return list_bucket(path)
        # logger.info("readdir: %s", path)
        # return list_bucket(path)


def pysssix_mount(mount_point, allow_other=False):
   return FUSE(S3FileSystemMount(),mount_point, foreground=True, allow_other=allow_other)