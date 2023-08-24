import logging

from twisted.internet import defer, reactor
from twisted.internet.protocol import Factory
from twisted.python import log
from twisted.trial import unittest

from stompest.asynchronous import Stomp
from stompest.asynchronous.listener import SubscriptionListener
from stompest.config import StompConfig
from stompest.error import StompCancelledError, StompConnectionError, StompProtocolError
from stompest.protocol import StompSpec

from .broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer, RemoteControlViaFrameStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncClientBaseTestCase(unittest.TestCase):
    protocols = []

    def setUp(self):
        self.connections = [self._create_connection(p) for p in self.protocols]

    def _create_connection(self, protocol):
        factory = Factory()
        factory.protocol = protocol
        connection = reactor.listenTCP(0, factory) # @UndefinedVariable
        return connection

    def tearDown(self):
        for connection in self.connections:
            connection.stopListening()

class AsyncClientConnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocols = [BlackHoleStompServer]
    TIMEOUT = 1.0

    @defer.inlineCallbacks
    def test_connection_timeout(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        try:
            yield client.connect(connectTimeout=1e-5)
        except StompConnectionError:
            pass
        else:
            raise Exception('Expected connect timeout, but connection was established.')

    @defer.inlineCallbacks
    def test_connected_timeout(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        try:
            yield client.connect(connectedTimeout=self.TIMEOUT)
        except StompCancelledError:
            pass
        else:
            raise Exception('Expected connected timeout, but connection was established.')

    @defer.inlineCallbacks
    def test_connected_timeout_after_failover(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=2,initialReconnectDelay=0,randomize=false' % port)
        client = Stomp(config)
        try:
            yield client.connect(connectedTimeout=self.TIMEOUT)
        except StompCancelledError:
            pass
        else:
            raise Exception('Expected connected timeout, but connection was established.')

    @defer.inlineCallbacks
    def test_not_connected(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        try:
            yield client.send('/queue/fake')
        except StompConnectionError:
            pass
        else:
            raise Exception('Expected connection error, but nothing frame could be sent.')

class AsyncClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocols = [ErrorOnConnectStompServer]

    @defer.inlineCallbacks
    def test_stomp_protocol_error_on_connect(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        try:
            yield client.connect()
        except StompProtocolError:
            pass
        else:
            raise Exception('Expected a StompProtocolError, but nothing was raised.')

class AsyncClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocols = [ErrorOnSendStompServer]

    @defer.inlineCallbacks
    def test_disconnect_on_stomp_protocol_error(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)

        yield client.connect()
        client.send('/queue/fake', b'fake message')
        try:
            yield client.disconnected
        except StompProtocolError:
            pass
        else:
            raise Exception('Expected a StompProtocolError, but nothing was raised.')

class AsyncClientFailoverOnDisconnectTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer, ErrorOnSendStompServer]

    @defer.inlineCallbacks
    def test_failover_on_connection_lost(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d,tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,randomize=false,maxReconnectAttempts=1' % ports)
        client = Stomp(config)

        yield client.connect()
        self.connections[0].stopListening()
        queue = '/queue/fake'
        client.send(queue, b'shutdown')
        try:
            yield client.disconnected
        except StompConnectionError:
            yield client.connect()
        else:
            raise Exception('Unexpected clean disconnect.')
        client.send(queue, b'fake message')

        try:
            yield client.disconnected
        except StompProtocolError:
            pass
        else:
            raise Exception('Unexpected clean disconnect.')

class AsyncClientReplaySubscriptionTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]

    @defer.inlineCallbacks
    def test_replay_after_failover(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,maxReconnectAttempts=1' % ports)
        client = Stomp(config)
        queue = '/queue/bla'
        try:
            client.subscribe(queue, listener=SubscriptionListener(self._on_message)) # client is not connected, so it won't accept subscriptions
        except StompConnectionError:
            pass
        else:
            raise Exception('Unexpected successful subscribe.')

        self.assertEquals(client.session._subscriptions, {}) # check that no subscriptions have been accepted
        yield client.connect()

        self.shutdown = True # the callback handler will kill the broker connection ...
        client.subscribe(queue, listener=SubscriptionListener(self._on_message))
        try:
            yield client.disconnected # the callback handler has killed the broker connection
        except StompConnectionError:
            pass
        else:
            raise Exception('Unexpected clean disconnect.')

        self.shutdown = False # the callback handler will not kill the broker connection, but callback self._got_message
        self._got_message = defer.Deferred()

        yield client.connect()
        self.assertNotEquals(client.session._subscriptions, []) # the subscriptions have been replayed ...

        result = yield self._got_message
        self.assertEquals(result, None) # ... and the message comes back

        yield client.disconnect()
        yield client.disconnected
        self.assertEquals(list(client.session.replay()), []) # after a clean disconnect, the subscriptions are forgotten.

    def _on_message(self, client, msg):
        self.assertTrue(isinstance(client, Stomp))
        self.assertEquals(msg.body, b'hi')
        if self.shutdown:
            client.send('/queue/fake', b'shutdown')
        else:
            self._got_message.callback(None)

class AsyncClientMultiSubscriptionsTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]

    @defer.inlineCallbacks
    def test_multi_subscriptions(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        yield client.connect()

        listeners = []
        for j in range(2):
            listener = SubscriptionListener(self._on_message)
            yield client.subscribe('/queue/%d' % j, headers={'bla': j}, listener=listener)
            listeners.append(listener)

        for (j, listener) in enumerate(listeners):
            self.assertEquals(listener._headers['bla'], j)

        yield client.disconnect()
        yield client.disconnected

    def _on_message(self, client, msg):
        pass

class AsyncClientDisconnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]

    @defer.inlineCallbacks
    def test_disconnect_timeout(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port, version='1.1')
        client = Stomp(config)
        yield client.connect()
        self._got_message = defer.Deferred()
        client.subscribe('/queue/bla', headers={StompSpec.ID_HEADER: 4711}, listener=SubscriptionListener(self._on_message, ack=False)) # we're acking the frames ourselves
        yield self._got_message
        try:
            yield client.disconnect(timeout=0.02)
        except StompCancelledError:
            pass
        else:
            raise Exception('Expected disconnect timeout, but disconnect went gracefully.')
        yield client.disconnected
        self.wait.callback(None)

    @defer.inlineCallbacks
    def test_disconnect_connection_lost_unexpectedly(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port, version='1.1')
        client = Stomp(config)

        yield client.connect()

        self._got_message = defer.Deferred()
        client.subscribe('/queue/bla', headers={StompSpec.ID_HEADER: 4711}, listener=SubscriptionListener(self._on_message, ack=False)) # we're acking the frames ourselves
        yield self._got_message

        disconnected = client.disconnected
        client.send('/queue/fake', b'shutdown') # tell the broker to drop the connection
        try:
            yield disconnected
        except StompConnectionError:
            pass
        else:
            raise Exception('Expected unexpected loss of connection, but disconnect went gracefully.')

        self.wait.callback(None)

    @defer.inlineCallbacks
    def _on_message(self, client, msg):
        client.nack(msg)
        self.wait = defer.Deferred()
        self._got_message.callback(None)
        yield self.wait

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
