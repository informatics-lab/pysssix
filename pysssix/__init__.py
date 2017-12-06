from .mount_s3 import pysssix_mount
from sys import argv, exit
import logging

logger = logging.getLogger('pysssix')

def main():
    
    if len(argv) not in [2, 3]:
        print('usage: %s <mountpoint> [--debug]' % argv[0])
        exit(1)

    if '--debug' in argv:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)

    logger.info("Starting up s3 fuse at mount point %s", argv[1])
    fuse = pysssix_mount(argv[1])