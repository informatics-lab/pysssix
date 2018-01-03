from .mount_s3 import S3FileSystemMount
# from .mount_s3 import S3FileSystemMount, S3Handeler
from .client import AsyncS3Mount
from .server import start_server
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

    args = parser.parse_args()

    if args.verbose:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)

    logger.info("Starting up s3 fuse at mount point %s", args.mount_point)
    fuse = FUSE(S3FileSystemMount(), args.mount_point, foreground=True, allow_other=args.allow_other)