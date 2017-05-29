import threading


class LocalProfileState(threading.local):
    def __init__(self):
        super(LocalProfileState, self).__init__()
        self.stats = []


_state = LocalProfileState()
globals()['_state'] = _state


def flush():
    out = _state.stats[:]
    _state.stats[:] = []
    return out


def append(stats):
    _state.stats.append(stats)


def reset():
    _state.stats[:] = []
