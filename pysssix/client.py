import asyncio
import pickle

@asyncio.coroutine
def echo_client(message, loop):
    reader, writer = yield from asyncio.open_unix_connection('pysssix', loop=loop)

    print('Send: %r' % message)
    writer.write(pickle.dumps(message))
    writer.write_eof()

    data = yield from reader.read()
    print('Received: %s bytes' % len(data))

    if(message['operation'] is not 'read' and len(data) > 0):
        print('Result:%s' % pickle.loads(data))
    print('Close the socket')
    writer.close()
    
loop = asyncio.get_event_loop()

message = {'operation':'open','path':'data-from-spice/20171201T0000Z/indon2km1p5/umnsaa_mlppd000'}
loop.run_until_complete(echo_client(message, loop))

message = {'operation':'read','fh':0, 'size':100 }
loop.run_until_complete(echo_client(message, loop))

message = {'operation':'release','fh': 0}
loop.run_until_complete(echo_client(message, loop))


loop.close()