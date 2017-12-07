import asynq
import pickle
from qcore.asserts import assert_eq


def test_constfuture_pickling():
    fut = asynq.ConstFuture(3)

    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        pickled = pickle.dumps(fut, protocol)
        assert_eq(fut.value(), pickle.loads(pickled).value())


def test_callback_exception_handling():
    def raise_exception():
        raise ValueError

    fut = asynq.Future(lambda: 3)
    fut.on_computed.subscribe(raise_exception)

    # exception is passed on and value gets set correctly
    assert_eq(3, fut.value())
