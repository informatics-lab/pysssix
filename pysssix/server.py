import asyncio
from .s3 import open as s3_open, getattr as s3_getattr, list_bucket # TODO: relative imports
import pickle
from functools import partial
import logging

# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)

clients = []

async def read(message, writer, fh_store):
    s3_reader = fh_store[message['fh']]
    offset = message.get('offset', None)
    size = message['size']
    do_read = partial(s3_reader.read, size, offset)
    data = await asyncio.get_event_loop().run_in_executor(None, do_read)
    for i, chunk in enumerate(data):
        logger.info("Send: chunk %d of %d chunks of data" % (i+1, len(data)))
        writer.write(chunk.read())

async def getattr(message, writer, fh_store):
    data = await asyncio.get_event_loop().run_in_executor(None, s3_getattr, message['path'])
    logger.debug("respond to getattr for %.100s with %.100s" % (message, data))
    writer.write(pickle.dumps(data))

async def open(message, writer, fh_store):
    fh = len(fh_store)
    fh_store[fh] = await asyncio.get_event_loop().run_in_executor(None,  partial(s3_open, message['path']))
    data = fh
    logger.debug("respond to open for %.100s with %.100s" % (message, data))
    writer.write(pickle.dumps(data))

async def readdir(message, writer, fh_store):
    data = await asyncio.get_event_loop().run_in_executor(None,  
    partial(list_bucket, message['path']))
    logger.debug("respond to readdir for %.100s with %.100s" % (message, data))
    writer.write(pickle.dumps(data))

async def release(message, writer, fh_store):
    try:
        del fh_store[message['fh']]
    except KeyError:
        pass
    data = b''
    logger.debug("respond to release for %.100s with %.100s" % (message, data))
    writer.write(pickle.dumps(data))

async def handle_s3_request(fh_store, reader, writer):
    try:
        print('request')
        request_data = await reader.read()
        message = pickle.loads(request_data)
        logger.debug("Received %r" % (message))
        operation = message['operation']
        logger.debug("run operation '%s'" % operation)
        
        operations = {
            'getattr':getattr,
            'open':open,
            'read':read,
            'readdir':readdir,
            'release':release
        }

        
        if operation not in operations.keys():
            raise ValueError("Invalid opporation %s. Valid options are %s" % (
                operation, ['getattr','open','read','readdir']))
    
        await operations[operation](message, writer, fh_store)
    
        await writer.drain()
        
    finally:
        logger.debug("Close the client socket")
        writer.close()


def start_server(port):
    fh_store = {}
    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(partial(handle_s3_request, fh_store), port=5472, loop=loop)
    #coro = asyncio.start_unix_server(partial(handle_s3_request, fh_store), 'pysssix', loop=loop)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    logger.info('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    print('closing down...')
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == '__main__':

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    logger.info("starting up..")
    start_server()

    