import asyncio
import pickle
import logging
from fuse import Operations

# Logging 
logger = logging.getLogger('pysssix')
logger.setLevel(logging.DEBUG)


async def message_server(message, loop, port):
    #reader, writer =  await asyncio.open_unix_connection('pysssix', loop=loop)
    reader, writer =  await asyncio.open_connection(port=port, loop=loop)
    
    logger.debug('Client send: %r' % message)
    writer.write(pickle.dumps(message))
    writer.write_eof()


    data = await reader.read()
    logger.debug('Client received: %s bytes' % len(data))

    writer.close()
    logger.debug('Client closed connection')
    return data



def message_and_wait(message, port):
    try:
        loop = asyncio.new_event_loop()
        print('new message...', __name__)
        result = loop.run_until_complete(message_server(message, loop, port))
        loop.close()
        print('loop done')
        return result
    except Exception as e:
        print(e)
        raise


class AsyncS3Mount(Operations):  
    def __init__(self, port):
        self.port = port      

    def flush(self, path, fh):
        message_and_wait({'operation':'release','fh': fh}, self.port)

    def getattr(self, path, fh=None):
        return pickle.loads(
                message_and_wait({'operation':'getattr','path': path}, self.port))

    def open(self, file, flags, mode=None):
        return pickle.loads(
                message_and_wait({'operation':'open','path': file}, self.port))

    def read(self, path, size, offset, fh):
        return message_and_wait({
                    'operation':'read',
                    'fh': fh,
                    'size':size, 
                    'offset':offset},
                self.port)

    def release(self, path, fh):
        return pickle.loads(
                message_and_wait({'operation':'release', 'fh': fh}, self.port))

    def readdir(self, path, fh):
        return pickle.loads(
                message_and_wait({'operation':'readdir', 'path': path}, self.port))


if __name__ == "__main__":

    file = 'data-from-spice/20171201T0000Z/indon2km1p5/umnsaa_mlppd000'
    file = 'theo-misc/num_10000.txt'
    client = AsyncS3Mount()
    print(client.readdir(file[:-4], 0))
    fh = client.open(file,[])
    data = client.read(file, 3000, 2, fh)
    print(data)
    client.release(file, fh)
