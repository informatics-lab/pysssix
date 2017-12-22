import asyncio
import s3 # TODO: relative imports
import pickle
from functools import partial
import logging

# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)

clients = []





@asyncio.coroutine
def handle_s3_request(loop, fh_store, reader, writer):
    try:
        request_data = yield from reader.read()
        message = pickle.loads(request_data)
        addr = writer.get_extra_info('peername')
        logger.debug("Received %r from %r" % (message, addr))

        operation = message['operation']
        logger.debug("run operation '%s'" % operation)
        
        if operation == 'getattr':
            data = yield from loop.run_in_executor(None,  partial(s3.getattr, message['path']))
            data = pickle.dumps(data)
        
        elif operation == 'open':
            fh = len(fh_store)
            fh_store[fh] = yield from loop.run_in_executor(None,  partial(s3.open, message['path']))
            data = fh
            data = pickle.dumps(data)
        
        elif operation == 'read':
            s3_reader = fh_store[message['fh']]
            data = yield from loop.run_in_executor(None,  
                partial(s3_reader.read, message['size'], offset=message.get('offset', None)))
        
        elif operation == 'readdir':
            data = yield from loop.run_in_executor(None,  
                partial(s3.readdir, message['path']))
            data = pickle.dumps(data)

        elif  operation == 'release':
            try:
                del fh_store[message['fh']]
            except KeyError:
                pass
            data = b''
        
        else:
            raise ValueError("Invalid opporation %s. Valid options are %s" % (
                operation, ['getattr','open','read','readdir']))
        

        if isinstance(data, (list, tuple)):
            for d in data:
                logger.info("Send: one chunk of data")
                writer.write(d.read())
        else:
            logger.info("Send: %.50s" % data)
            writer.write(data)
        yield from writer.drain()
        
    finally:
        logger.debug("Close the client socket")
        writer.close()


if __name__ == '__main__':

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    logger.info("starting up..")


    fh_store = {}
    loop = asyncio.get_event_loop()
    coro = asyncio.start_unix_server(partial(handle_s3_request, loop, fh_store), 'pysssix', loop=loop)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    logger.info('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()