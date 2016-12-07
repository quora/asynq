# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import contextlib
import sys
import qcore
import inspect
import linecache
import traceback
import logging
from sys import stderr, stdout

from . import _debug

options = _debug.options  # Must be the same object

options.DUMP_PRE_ERROR_STATE  = True
options.DUMP_EXCEPTIONS       = False
options.DUMP_AWAIT_RECURSION  = False
options.DUMP_SCHEDULE_TASK    = False
options.DUMP_CONTINUE_TASK    = False
options.DUMP_SCHEDULE_BATCH   = False
options.DUMP_FLUSH_BATCH      = False
options.DUMP_DEPENDENCIES     = False
options.DUMP_COMPUTED         = False
options.DUMP_NEW_TASKS        = False
options.DUMP_YIELD_RESULTS    = False
options.DUMP_QUEUED_RESULTS   = False
options.DUMP_CONTEXTS         = False
options.DUMP_SCHEDULER_CHANGE = False
options.DUMP_SYNC             = False
options.DUMP_PRIMER           = False
options.DUMP_STACK            = False  # When it's meaningful, e.g. on batch flush
options.DUMP_SCHEDULER_STATE  = False

options.SCHEDULER_STATE_DUMP_INTERVAL = 1    # In seconds
options.DEBUG_STR_REPR_MAX_LENGTH     = 240  # In characters, 0 means infinity
options.STACK_DUMP_LIMIT              = 10   # In frames, None means infinity

options.ENABLE_COMPLEX_ASSERTIONS = True

def DUMP_ALL(value=None):
    return options.DUMP_ALL(value)

# DUMP_ALL(True)


original_hook = None
is_attached = False
_std_str = str
_std_repr = repr


def dump_error(error, tb=None):
    """Dumps errors w/async stack traces."""
    try:
        stderr.write('\n' + (format_error(error, tb=tb) or 'No error'))
    finally:
        stdout.flush()
        stderr.flush()


def format_error(error, tb=None):
    """Formats errors w/async stack traces."""
    if error is None:
        return None
    result = ''
    if hasattr(error, '_traceback') or tb is not None:
        tb = tb or error._traceback
        result += '\n\nTraceback:\n%s' % ''.join(format_tb(tb))
    if isinstance(error, BaseException):
        result += '\n' + ''.join(traceback.format_exception_only(error.__class__, error))
    return result


class AsynqStackTracebackFormatter(logging.Formatter):
    """Prints traceback skipping asynq frames during logger.exception/error usages."""
    def formatException(self, exc_info):
        ty, val, tb = exc_info
        return format_error(val, tb=tb)


def _should_skip_frame(frame):
    if frame:
        traceback_hide_directive_name = '__traceback_hide__'
        return frame.f_locals.get(
            traceback_hide_directive_name,
            frame.f_globals.get(
                traceback_hide_directive_name,
                False)) is True
    return False


def extract_tb(tb, limit=None):
    """This implementation is stolen from traceback module but respects __traceback_hide__."""
    if limit is None:
        if hasattr(sys, 'tracebacklimit'):
            limit = sys.tracebacklimit
    tb_list = []
    n = 0
    while tb is not None and (limit is None or n < limit):
        f = tb.tb_frame
        if not _should_skip_frame(f):
            lineno = tb.tb_lineno
            co = f.f_code
            filename = co.co_filename
            name = co.co_name
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            if line:
                line = line.strip()
            else:
                line = None
            tb_list.append((filename, lineno, name, line))
        tb = tb.tb_next
        n = n + 1
    return tb_list


def format_tb(tb):
    """Formats a tracebacck but skips """
    return traceback.format_list(extract_tb(tb))


def dump_stack(skip=0, limit=None):
    """Dumps current stack trace."""
    skip += 2  # To skip dump_stack and traceback.extract_stack
    if limit is None:
        limit = options.STACK_DUMP_LIMIT
    print('--- Stack trace: -----------------------------------------------------')
    try:
        stack = traceback.extract_stack(limit=None if limit is None else limit + skip)
        print(''.join(traceback.format_list(stack[:-skip])), end='')
    finally:
        print('----------------------------------------------------------------------')
        stdout.flush()


def dump_asynq_stack():
    """Dumps the current asynq stack to stdout."""
    # Doing this in the global scope creates a circular dependency
    from .scheduler import get_scheduler
    active_task = get_scheduler().active_task
    if active_task is not None:
        print('\n'.join(active_task.traceback()))
    else:
        print('dump_asynq_stack: no asynq task currently active')


def dump(state):
    if not options.DUMP_PRE_ERROR_STATE:
        return
    stdout.flush()
    stderr.flush()
    stdout.write('\n--- Pre-error state dump: --------------------------------------------\n')
    try:
        state.dump()
    finally:
        stdout.write('----------------------------------------------------------------------\n')
        stderr.write('\n')
        stdout.flush()
        stderr.flush()


def write(text, indent=0):
    if indent > 0:
        indent_str = '  ' * indent
        text = text.replace('\n', '\n' + indent_str)
        if not text.startswith('\n'):
            text = indent_str + text
    stdout.write(text + '\n')


def str(source, truncate=True):
    return qcore.safe_str(source, options.DEBUG_STR_REPR_MAX_LENGTH if truncate else 0)


def repr(source, truncate=True):
    return qcore.safe_repr(source, options.DEBUG_STR_REPR_MAX_LENGTH if truncate else 0)


def async_exception_hook(type, error, tb):
    """Exception hook capable of printing async stack traces."""
    global original_hook

    stdout.flush()
    stderr.flush()
    if original_hook is not None:
        original_hook(type, error, tb)
    dump_error(error, tb=tb)


def ipython_custom_exception_handler(self, etype, value, tb, tb_offset=None):
    """Override ipython's exception handler to print async traceback."""
    async_exception_hook(etype, value, tb)
    # below is the default exception handling behavior of ipython
    self.showtraceback()


def attach_exception_hook():
    """Injects async exception hook into the sys.excepthook."""
    try:
        # detect whether we're running in IPython
        __IPYTHON__
    except NameError:
        shell = None
    else:
        # override ipython's exception handler if in a shell.
        # we need to do this because ipython overrides sys.excepthook
        # so just the else block doesn't cover that case.
        from IPython.core.getipython import get_ipython

        # this may be None if __IPYTHON__ is somehow defined, but we are not
        # in fact in a shell
        shell = get_ipython()

    if shell is not None:
        shell.set_custom_exc((BaseException,), ipython_custom_exception_handler)
    else:
        global is_attached, original_hook
        if is_attached:
            sys.stderr.write("Warning: async exception hook was already attached.\n")
            return
        original_hook = sys.excepthook
        sys.excepthook = async_exception_hook
        is_attached = True


def detach_exception_hook():
    """Removes async exception hook into the sys.excepthook."""
    global is_attached, original_hook
    assert is_attached, "Async exception hook wasn't attached."
    sys.excepthook = original_hook
    is_attached = False


@contextlib.contextmanager
def enable_complex_assertions(enable=True):
    old = options.ENABLE_COMPLEX_ASSERTIONS
    try:
        options.ENABLE_COMPLEX_ASSERTIONS = enable
        yield None
    finally:
        options.ENABLE_COMPLEX_ASSERTIONS = old


def disable_complex_assertions():
    return enable_complex_assertions(False)


def sync():
    assert False, "'import asynq' seems broken: this function must be replaced with async.batching.sync."


def get_frame_info(generator):
    """Given a generator, returns its current frame info."""
    if getattr(generator, 'gi_frame', None) is not None:
        return inspect.getframeinfo(generator.gi_frame)
    return None
