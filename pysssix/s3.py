from __future__ import print_function, absolute_import, division
import logging
from functools  import lru_cache
import boto3
from botocore.exceptions import ClientError
# from .block_cache import BlockCache 
from fuse import FuseOSError
from errno import ENOENT

s3 = boto3.client('s3')


# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)

@lru_cache(1024)
def get_size(path):
    bucket, key = parse_path(path)
    response = s3.head_object(Bucket=bucket, Key=key)
    return response['ContentLength']

def range_string(start, stop):
        return "bytes={:d}-{:d}".format(start, stop)

def parse_path(path):
    path = path[1:] if path[0] == '/' else path 
    parts = path.split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])
    return bucket, key

def get_bytes(path, start, stop):
    rng=range_string(start, stop)
    bucket, key = parse_path(path)
    logger.info("Request %s between %s", path, rng)
    return s3.get_object(Bucket=bucket, Key=key, Range=rng)['Body'].read()

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
        s3.list_objects_v2(Bucket=bucket,Prefix=key,MaxKeys=1)['Contents']
    except KeyError:
        raise FuseOSError(ENOENT)

    # Test if path represents a complete bucket, key pair.
    try:
        if get_size(path) <= 0:
            raise ValueError("Content empty")
        return 2 # Object exists. It's a file.
    except ValueError as e:
        raise FuseOSError(ENOENT)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return 1 # The key doesn't exist so treat as a directory
        else:
            raise # Something else has gone wrong.


@lru_cache(1024)
def getattr(path):
    return {'st_mode': 33188, 'st_size': get_size(path)} if obj_type(path) == 2 else {'st_mode': 16877}

def list_bucket(path):
    logger.info("Requested ls for %s", path)
    bucket, key = parse_path(path)
    
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