#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock
import smart_open

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

# os.environ['S3_USE_SIGV4'] = 'True' # TODO: Is this the best place...


import boto3
s3 = boto3.resource('s3')


def open(path, mode='rb', *args, **kwargs):
    if mode != 'rb':
        raise ValueError("s3reader only works in binary read ('rb') mode")
    bucket, key = parse_path(path)
    return S3Reader(bucket, key)


def parse_path(path):
    if not path.startswith("s3://"):
        raise ValueError("s3reader.open requires an s3:// URI")
    path = path.replace("s3://", "")
    parts = path.split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])
    return bucket, key


class S3Reader(object):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.obj = s3.Object(self.bucket, self.key)
        self.size = self.obj.content_length
        self.pos = 0  # pointer to starting read position

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.pos = 0

    def range_string(self, start, stop):
        return "bytes={}-{}".format(start, stop)

    def read(self, nbytes=None):
        if not nbytes:
            nbytes = self.size - self.pos
        # TODO confirm that start and stop bytes are within 0 to size
        rng = self.range_string(self.pos, self.pos + nbytes - 1)
        self.pos += nbytes  # TODO wait to move pointer until read confirmed
        return self.obj.get(Range=rng)['Body'].read()

    def seek(self, offset, whence=0):
        self.pos = whence + offset



class S3FileSystemMount(LoggingMixIn, Operations):
    def __init__(self):
        self.filecount = 0;
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
            return {'st_mode': 33188, 'st_size': open('s3:/' + path).size}
        else :
            return {'st_mode': 16877}
        
        
   
    def open(self, file, flags, mode=None):
        # TODO: check flags ? 
        fh = self.filecount;
        self.filecount += 1;
        path = "s3:/" + file
        logging.info(path);
        self.openfh[fh] = open(path)
        return fh

    def read(self, path, size, offset, fh):
        logging.info([self, path, size, offset, fh]);
        self.openfh[fh].seek(offset)
        return self.openfh[fh].read(size)

    def release(self, path, fh):
        del self.openfh[fh]
        return


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)

    fuse = FUSE(S3FileSystemMount(),argv[1], foreground=True)