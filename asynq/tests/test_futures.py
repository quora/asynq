import asynq
import pickle
from qcore.asserts import assert_eq, AssertRaises


def test_constfuture_pickling():
    fut = asynq.ConstFuture(3)

    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        pickled = pickle.dumps(fut, protocol)
        assert_eq(fut.value(), pickle.loads(pickled).value())


def test_callback_exception_handling():
    def raise_exception(task):
        raise ValueError

    def base_exception(task):
        raise KeyboardInterrupt

    fut_exc = asynq.Future(lambda: 3)
    fut_exc.on_computed.subscribe(raise_exception)

    fut_base_exc = asynq.Future(lambda: 4)
    fut_base_exc.on_computed.subscribe(base_exception)

    # exception is passed on and value gets set correctly
    assert_eq(3, fut_exc.value())

    # since there's a base exception, we fail when we try to set the value to the future
    with AssertRaises(KeyboardInterrupt):
        assert_eq(4, fut_base_exc.value())
