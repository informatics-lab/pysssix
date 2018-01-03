from .s3_fuse_ops import S3FUSEOps
from fuse import FUSE
from sys import argv, exit
import logging
import argparse

logger = logging.getLogger('pysssix')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mount_point", help="where to mount S3")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("-a", "--allow_other", help="pass allow_other=True to FUSE", action="store_true")
    parser.add_argument("-p", "--port", help="port to use for client server communication", default=5472,  type=int)
    parser.add_argument("-c", "--cache_size", help="Approximate size of the cache in bytes", default=4e9,  type=float)
    parser.add_argument("-b", "--block_size", help="Block size for requests", default=8192,  type=float)
    parser.add_argument("-l", "--cache_location", help="Path to disk location to store cache", default='~/.pyssssix_cache')

    args = parser.parse_args()

    if args.verbose:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)

    logger.info("Starting up s3 fuse at mount point %s", args.mount_point)
    fuse = FUSE(
                S3FUSEOps(cache_size=args.cache_size, block_size=args.block_size, cache_path=args.cache_location),
                args.mount_point, foreground=True, allow_other=args.allow_other)