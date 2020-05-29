import threading


class LocalProfileState(threading.local):
    def __init__(self):
        super(LocalProfileState, self).__init__()
        self.stats = []
        self.counter = 0


_state = LocalProfileState()
globals()["_state"] = _state


def flush():
    out = _state.stats[:]
    reset()
    return out


def append(stats):
    _state.stats.append(stats)


def reset():
    _state.stats[:] = []
    _state.counter = 0


def incr_counter():
    _state.counter = _state.counter + 1
    return _state.counter
