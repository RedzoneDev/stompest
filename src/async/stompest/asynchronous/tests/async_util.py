import logging

from twisted.internet import defer, reactor
from twisted.internet.defer import CancelledError
from twisted.trial import unittest

from stompest.asynchronous.util import InFlightOperations
from stompest.error import StompCancelledError

logging.basicConfig(level=logging.DEBUG)

LOG_CATEGORY = __name__

class InFlightOperationsTest(unittest.TestCase):
    def test_dict_interface(self):
        op = InFlightOperations('test')
        self.assertEquals(list(op), [])
        self.assertRaises(KeyError, op.__getitem__, 1)
        self.assertRaises(KeyError, lambda: op[1])
        self.assertRaises(KeyError, op.pop, 1)
        self.assertIdentical(op.get(1), None)
        self.assertIdentical(op.get(1, 2), 2)
        op[1] = w = defer.Deferred()
        self.assertEquals(list(op), [1])
        self.assertIdentical(op[1], w)
        self.assertIdentical(op.get(1), w)
        self.assertRaises(KeyError, op.__setitem__, 1, defer.Deferred())
        self.assertIdentical(op.pop(1), w)
        self.assertRaises(KeyError, op.pop, 1)
        op[1] = w
        self.assertEquals(op.popitem(), (1, w))
        self.assertEquals(list(op), [])
        self.assertIdentical(op.setdefault(1, w), w)
        self.assertIdentical(op.setdefault(1, w), w)

    @defer.inlineCallbacks
    def test_context_single(self):
        op = InFlightOperations('test')
        with op(1) as w:
            self.assertEquals(list(op), [1])
            self.assertIsInstance(w, defer.Deferred)
            self.assertIdentical(w, op[1])
            self.assertIdentical(op.get(1), op[1])
        self.assertEquals(list(op), [])

        with op(key=2, log=logging.getLogger(LOG_CATEGORY)):
            self.assertEquals(list(op), [2])
            self.assertIsInstance(op.get(2), defer.Deferred)
            self.assertIdentical(op.get(2), op[2])
        self.assertEquals(list(op), [])

        try:
            with op(None, logging.getLogger(LOG_CATEGORY)) as w:
                reactor.callLater(0, w.cancel) # @UndefinedVariable
                yield w.wait(timeout=None, fail=None)
        except CancelledError:
            pass
        else:
            raise
        self.assertEquals(list(op), [])

        try:
            with op(None, logging.getLogger(LOG_CATEGORY)) as w:
                reactor.callLater(0, w.errback, StompCancelledError('4711')) # @UndefinedVariable
                yield w.wait()
        except StompCancelledError as e:
            self.assertEquals(str(e), '4711')
        else:
            raise
        self.assertEquals(list(op), [])

        with op(None, logging.getLogger(LOG_CATEGORY)) as w:
            reactor.callLater(0, w.callback, 4711) # @UndefinedVariable
            result = yield w.wait()
            self.assertEquals(result, 4711)
        self.assertEquals(list(op), [])

        try:
            with op(None) as w:
                raise RuntimeError('hi')
        except RuntimeError:
            pass
        self.assertEquals(list(op), [])
        try:
            yield w
        except RuntimeError as e:
            self.assertEquals(str(e), 'hi')
        else:
            raise

        try:
            with op(None) as w:
                d = w.wait()
                raise RuntimeError('hi')
        except RuntimeError:
            pass
        self.assertEquals(list(op), [])
        try:
            yield d
        except RuntimeError as e:
            self.assertEquals(str(e), 'hi')
        else:
            pass

    @defer.inlineCallbacks
    def test_timeout(self):
        op = InFlightOperations('test')
        with op(None) as w:
            try:
                yield w.wait(timeout=0, fail=RuntimeError('hi'))
            except RuntimeError as e:
                self.assertEquals(str(e), 'hi')
            else:
                raise
            self.assertEquals(list(op), [None])
        self.assertEquals(list(op), [])

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
