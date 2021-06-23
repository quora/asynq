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

import contextlib
import sys
import qcore
import linecache
import traceback
import logging
from sys import stderr, stdout
from pygments import highlight  # pygments loading behavior requires this style import
from pygments import lexers  # see: https://github.com/montylounge/django-sugar/issues/2
from pygments import formatters

from . import _debug

options = _debug.options  # Must be the same object

options.DUMP_PRE_ERROR_STATE = True
options.DUMP_EXCEPTIONS = False
options.DUMP_SCHEDULE_TASK = False
options.DUMP_CONTINUE_TASK = False
options.DUMP_SCHEDULE_BATCH = False
options.DUMP_FLUSH_BATCH = False
options.DUMP_DEPENDENCIES = False
options.DUMP_COMPUTED = False
options.DUMP_NEW_TASKS = False
options.DUMP_YIELD_RESULTS = False
options.DUMP_QUEUED_RESULTS = False
options.DUMP_CONTEXTS = False
options.DUMP_SYNC = False
options.DUMP_STACK = False  # When it's meaningful, e.g. on batch flush
options.DUMP_SCHEDULER_STATE = False
options.DUMP_SYNC_CALLS = False
options.COLLECT_PERF_STATS = False

options.SCHEDULER_STATE_DUMP_INTERVAL = 1  # In seconds
options.DEBUG_STR_REPR_MAX_LENGTH = 240  # In characters, 0 means infinity
options.STACK_DUMP_LIMIT = 10  # In frames, None means infinity
options.MAX_TASK_STACK_SIZE = 1000000  # Max number of concurrent futures + batch items

options.ENABLE_COMPLEX_ASSERTIONS = True
options.KEEP_DEPENDENCIES = False  # don't clear dependencies between yields


def DUMP_ALL(value=None):
    return options.DUMP_ALL(value)


# DUMP_ALL(True)


original_hook = None
is_attached = False
_use_original_exc_handler = False
_should_filter_traceback = True
_use_syntax_highlighting = True
_std_str = str
_std_repr = repr


def enable_original_exc_handler(enable):
    """Enable/disable the original exception handler

    This mainly controls how exceptions are printed when an exception is thrown.
    Asynq overrides the exception handler to better display asynq stacktraces,
    but in some circumstances you may want to show original traces.

    For example, in Jupyter notebooks, the default exception handler displays
    context on exception lines. Enable this function if you'd like that behavior.

    """
    global _use_original_exc_handler
    _use_original_exc_handler = enable


def enable_filter_traceback(enable):
    """Enable/disable replacing asynq boilerplate lines in stacktraces.

    These lines are repeated many times in stacktraces of codebases using asynq.
    By default we replace them so it's easier to read the stacktrace, but you can enable
    if you're debugging an issue where it's useful to know the exact lines.

    """
    global _should_filter_traceback
    _should_filter_traceback = enable


def enable_traceback_syntax_highlight(enable):
    """Enable/disable syntax highlighted stacktraces when using asynq's exception handler."""
    global _use_syntax_highlighting
    _use_syntax_highlighting = enable


def dump_error(error, tb=None):
    """Dumps errors w/async stack traces."""
    try:
        stderr.write("\n" + (format_error(error, tb=tb) or "No error"))
    finally:
        stdout.flush()
        stderr.flush()


def format_error(error, tb=None):
    """Formats errors w/async stack traces."""
    if error is None:
        return None
    result = ""
    if hasattr(error, "_traceback") or tb is not None:
        tb = tb or error._traceback
        tb_list = traceback.format_exception(error.__class__, error, tb)
    elif isinstance(error, BaseException):
        tb_list = traceback.format_exception_only(error.__class__, error)
    else:
        tb_list = []

    tb_text = "".join(tb_list)

    if isinstance(tb_text, bytes):
        tb_text = tb_text.decode("utf-8", "replace")

    if _use_syntax_highlighting:
        tb_text = syntax_highlight_tb(tb_text)

    if _should_filter_traceback:
        # need to do this after syntax highlighting, so we turn it back into a list
        tb_text = "".join(filter_traceback(tb_text.splitlines(True)))

    result += tb_text
    return result


class AsynqStackTracebackFormatter(logging.Formatter):
    """Prints traceback skipping asynq frames during logger.exception/error usages."""

    def formatException(self, exc_info):
        ty, val, tb = exc_info
        return format_error(val, tb=tb)


def _should_skip_frame(frame):
    if frame:
        traceback_hide_directive_name = "__traceback_hide__"
        return (
            frame.f_locals.get(
                traceback_hide_directive_name,
                frame.f_globals.get(traceback_hide_directive_name, False),
            )
            is True
        )
    return False


def extract_tb(tb, limit=None):
    """This implementation is stolen from traceback module but respects __traceback_hide__."""
    if limit is None:
        if hasattr(sys, "tracebacklimit"):
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
    """Formats a traceback into a list of lines."""
    return traceback.format_list(extract_tb(tb))


def dump_stack(skip=0, limit=None):
    """Dumps current stack trace."""
    skip += 2  # To skip dump_stack and traceback.extract_stack
    if limit is None:
        limit = options.STACK_DUMP_LIMIT
    print("--- Stack trace: -----------------------------------------------------")
    try:
        stack = traceback.extract_stack(limit=None if limit is None else limit + skip)
        print("".join(traceback.format_list(stack[:-skip])), end="")
    finally:
        print("----------------------------------------------------------------------")
        stdout.flush()


def dump_asynq_stack():
    """Dumps the current asynq stack to stdout."""
    format_list = format_asynq_stack()
    if format_list is None:
        print("dump_asynq_stack: no asynq task currently active")
    else:
        print("\n".join(format_list))


def format_asynq_stack():
    """Returns a list of strings.

    Each string corresponds to one item in the current asynq stack.

    Returns None if there is no active asynq task.

    """
    # Doing this in the global scope creates a circular dependency
    from .scheduler import get_scheduler

    active_task = get_scheduler().active_task
    if active_task is not None:
        return active_task.traceback()
    else:
        return None


def dump(state):
    if not options.DUMP_PRE_ERROR_STATE:
        return
    stdout.flush()
    stderr.flush()
    stdout.write(
        "\n--- Pre-error state dump: --------------------------------------------\n"
    )
    try:
        state.dump()
    finally:
        stdout.write(
            "----------------------------------------------------------------------\n"
        )
        stderr.write("\n")
        stdout.flush()
        stderr.flush()


def write(text, indent=0):
    if indent > 0:
        indent_str = "  " * indent
        text = text.replace("\n", "\n" + indent_str)
        if not text.startswith("\n"):
            text = indent_str + text
    stdout.write(text + "\n")


def str(source, truncate=True):
    return qcore.safe_str(source, options.DEBUG_STR_REPR_MAX_LENGTH if truncate else 0)


def repr(source, truncate=True):
    return qcore.safe_repr(source, options.DEBUG_STR_REPR_MAX_LENGTH if truncate else 0)


def async_exception_hook(type, error, tb):
    """Exception hook capable of printing async stack traces."""

    stdout.flush()
    stderr.flush()
    if _use_original_exc_handler and original_hook is not None:
        original_hook(type, error, tb)
    dump_error(error, tb=tb)


def ipython_custom_exception_handler(self, etype, value, tb, tb_offset=None):
    """Override ipython's exception handler to print async traceback."""
    async_exception_hook(etype, value, tb)
    # below is the default exception handling behavior of ipython
    if _use_original_exc_handler:
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
    assert False, (
        "'import asynq' seems broken: this function must be replaced with"
        " asynq.batching.sync."
    )


def get_frame(generator):
    """Given a generator, returns its current frame."""
    if getattr(generator, "gi_frame", None) is not None:
        return generator.gi_frame
    return None


def filter_traceback(tb_list):
    """Given a traceback as a list of strings, looks for common boilerplate and removes it."""

    # List of boiler plate pattern to look for, with example before each
    # The match pattern is just some string that the line needs to contain
    """
        File "asynq/async_task.py", line 169, in asynq.async_task.AsyncTask._continue
        File "asynq/async_task.py", line 237, in asynq.async_task.AsyncTask._continue_on_generator
        File "asynq/async_task.py", line 209, in asynq.async_task.AsyncTask._continue_on_generator
    """
    TASK_CONTINUE = (
        [
            "asynq.async_task.AsyncTask._continue",
            "asynq.async_task.AsyncTask._continue_on_generator",
            "asynq.async_task.AsyncTask._continue_on_generator",
        ],
        "___asynq_continue___",
    )

    """
        File "asynq/decorators.py", line 161, in asynq.decorators.AsyncDecorator.__call__
        File "asynq/futures.py", line 54, in asynq.futures.FutureBase.value
        File "asynq/futures.py", line 63, in asynq.futures.FutureBase.value
        File "asynq/futures.py", line 153, in asynq.futures.FutureBase.raise_if_error
        File "<...>/python3.6/site-packages/qcore/errors.py", line 93, in reraise
           six.reraise(type(error), error, error._traceback)
        File "<...>/python3.6/site-packages/six.py", line 693, in reraise
           raise value
    """
    FUTURE_BASE = (
        [
            "asynq.decorators.AsyncDecorator.__call__",
            "asynq.futures.FutureBase.value",
            "asynq.futures.FutureBase.value",
            "asynq.futures.FutureBase.raise_if_error",
            "reraise",
            "six.reraise",
            "reraise",
            "value",
        ],
        "___asynq_future_raise_if_error___",
    )

    """
        File "asynq/decorators.py", line 153, in asynq.decorators.AsyncDecorator.asynq
        File "asynq/decorators.py", line 203, in asynq.decorators.AsyncProxyDecorator._call_pure
        File "asynq/decorators.py", line 203, in asynq.decorators.AsyncProxyDecorator._call_pure
        File "asynq/decorators.py", line 204, in asynq.decorators.AsyncProxyDecorator._call_pure
        File "asynq/decorators.py", line 275, in asynq.decorators.async_call
    """
    CALL_PURE = (
        [
            "asynq.decorators.AsyncDecorator.asynq",
            "asynq.decorators.AsyncProxyDecorator._call_pure",
            "asynq.decorators.AsyncProxyDecorator._call_pure",
            "asynq.decorators.AsyncProxyDecorator._call_pure",
            "asynq.decorators.async_call",
        ],
        "___asynq_call_pure___",
    )

    REPLACEMENTS = [TASK_CONTINUE, FUTURE_BASE, CALL_PURE]

    # iterate through the lines of the traceback and replace multiline
    # segments that match one of the replacements
    output = []
    i = 0
    while i < len(tb_list):
        did_replacement = False
        # for each replacement, try checking if all lines match
        # if so, replace with the given replacement
        for (text_to_match, replacement) in REPLACEMENTS:
            matches = True
            j = 0
            while j < len(text_to_match) and (i + j) < len(tb_list):
                if text_to_match[j] not in tb_list[i + j]:
                    matches = False
                    break
                j += 1
            if matches and j == len(text_to_match):
                # formatted to match default indentation level.
                output.append("  " + replacement + "\n")
                i = i + j
                did_replacement = True
                break
        if not did_replacement:
            output.append(tb_list[i])
            i += 1
    return output


def syntax_highlight_tb(tb_text):
    """Syntax highlights the traceback so that's a little easier to parse."""
    lexer = lexers.get_lexer_by_name("pytb", stripall=True)
    return highlight(tb_text, lexer, formatters.TerminalFormatter())
