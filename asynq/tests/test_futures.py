import asynq
import pickle
from qcore.asserts import assert_eq


def test_constfuture_pickling():
    fut = asynq.ConstFuture(3)

    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        pickled = pickle.dumps(fut, protocol)
        assert_eq(fut.value(), pickle.loads(pickled).value())
