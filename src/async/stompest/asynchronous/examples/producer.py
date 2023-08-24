import json
import logging

from twisted.internet import defer, task

from stompest.config import StompConfig

from stompest.asynchronous import Stomp
from stompest.asynchronous
class Producer(object):
    QUEUE = '/queue/testIn'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('tcp://localhost:61613')
        self.config = config

    @defer.inlineCallbacks
    def run(self, _):
        client = Stomp(self.config)
        yield client.connect()
        client.add(ReceiptListener(1.0))
        for j in range(10):
            yield client.send(self.QUEUE, json.dumps({'count': j}).encode(), receipt='message-%d' % j)
        client.disconnect(receipt='bye')
        yield client.disconnected # graceful disconnect: waits until all receipts have arrived

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    task.react(Producer().run)
