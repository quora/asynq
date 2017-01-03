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

from __future__ import absolute_import, division, print_function

import sys
import traceback
import logging

from six import StringIO
from qcore.asserts import assert_eq, assert_in, assert_is, assert_is_not
from qcore import MarkerObject

import asynq
from asynq import async


def test_dump_error():
    buf = StringIO()

    with asynq.mock.patch('asynq.debug.stderr', buf):
        asynq.debug.dump_error(None)
    assert_eq('\nNo error', buf.getvalue())


@async()
def async_fn():
    pass


def test_format_error():
    assert_is(None, asynq.debug.format_error(None))

    e = RuntimeError()
    expected = '\nRuntimeError\n'
    assert_eq(expected, asynq.debug.format_error(e))

    e._task = async_fn.async()
    formatted = asynq.debug.format_error(e)
    assert_in(expected, formatted)

    try:
        raise RuntimeError
    except RuntimeError:
        e._traceback = sys.exc_info()[2]

    formatted = asynq.debug.format_error(e)
    assert_in(expected, formatted)
    assert_in('Traceback', formatted)


def test_dump_stack():
    buf = StringIO()

    with asynq.mock.patch('sys.stdout', buf):
        def inner():
            asynq.debug.dump_stack()
        inner()

    printed = buf.getvalue()
    assert_in('test_dump_stack', printed)
    assert_in('Stack trace:', printed)


def test_dump_asynq_stack():
    @async()
    def caller():
        yield
        asynq.debug.dump_asynq_stack()

    buf = StringIO()

    with asynq.mock.patch('sys.stdout', buf):
        caller()

    printed = buf.getvalue()
    assert_in('caller', printed)


def _assert_write_result(text, indent, expected):
    buf = StringIO()

    with asynq.mock.patch('asynq.debug.stdout', buf):
        asynq.debug.write(text, indent=indent)

    assert_eq(expected, buf.getvalue())


def test_write():
    _assert_write_result('capybara', 0, 'capybara\n')
    _assert_write_result('capybara', 1, '  capybara\n')
    _assert_write_result('capybara', 2, '    capybara\n')
    _assert_write_result('nutria\ncapybara', 1, '  nutria\n  capybara\n')
    _assert_write_result('', 1, '  \n')
    _assert_write_result('\n', 1, '\n  \n')
    _assert_write_result('\ncapybara', 1, '\n  capybara\n')


def non_async_function_that_raises_an_error():
    raise ValueError


@async()
def async_function_that_raises_an_error():
    yield
    non_async_function_that_raises_an_error()


@async()
def async_function_whose_child_async_task_will_throw_an_error():
    yield async_function_that_raises_an_error.async()


def test_asynq_traceback_gets_glued_at_each_task_level():
    # tests that exceptions are producting the right tracebacks
    traceback_to_verify = None
    try:
        async_function_whose_child_async_task_will_throw_an_error()
    except ValueError:
        traceback_to_verify = sys.exc_info()[2]
    assert_is_not(None, traceback_to_verify)
    traceback_printed = '\n'.join(traceback.format_tb(traceback_to_verify))
    assert_in(non_async_function_that_raises_an_error.__name__, traceback_printed)
    assert_in(async_function_that_raises_an_error.__name__, traceback_printed)
    assert_in(async_function_whose_child_async_task_will_throw_an_error.__name__, traceback_printed)


def assert_eq_extracted_traceback_entry(
        entry,
        filename,
        fn_name,
        line):
    entry_filename, _, entry_fn_name, entry_line = entry
    assert_in(filename.rstrip('c'), entry_filename)
    assert_eq(fn_name, entry_fn_name)
    assert_eq(line, entry_line)


def test_extract_traceback():
    traceback_to_verify = None
    try:
        async_function_whose_child_async_task_will_throw_an_error()
    except ValueError:
        traceback_to_verify = sys.exc_info()[2]
    extracted_traceback_to_verify = asynq.debug.extract_tb(
            traceback_to_verify)

    this_level = extracted_traceback_to_verify[0]
    assert_eq_extracted_traceback_entry(
        this_level,
        __file__,
        'test_extract_traceback',
        'async_function_whose_child_async_task_will_throw_an_error()')

    # now check the last 3 frames. Between the 1st and the last 3 frames there may
    # or may not be a frame from six.reraise depending on the python version.
    async_wrapper_level = extracted_traceback_to_verify[-3]
    assert_eq_extracted_traceback_entry(
        async_wrapper_level,
        __file__,
        'async_function_whose_child_async_task_will_throw_an_error',
        'yield async_function_that_raises_an_error.async()')

    async_raiser_level = extracted_traceback_to_verify[-2]
    assert_eq_extracted_traceback_entry(
        async_raiser_level,
        __file__,
        'async_function_that_raises_an_error',
        'non_async_function_that_raises_an_error()')

    normal_function_raising_exception_level = extracted_traceback_to_verify[-1]
    assert_eq_extracted_traceback_entry(
        normal_function_raising_exception_level,
        __file__,
        'non_async_function_that_raises_an_error',
        'raise ValueError')


a_return_value = MarkerObject(u'A return value.')


@asynq.mock.patch('asynq.debug.traceback.format_list')
@asynq.mock.patch('asynq.debug.extract_tb')
def test_format_tb(mock_extract_tb, mock_format_list):
    mock_extract_tb.return_value = a_return_value
    mock_format_list.side_effect = lambda arg: arg
    traceback_to_verify = None
    try:
        async_function_whose_child_async_task_will_throw_an_error()
    except ValueError:
        traceback_to_verify = sys.exc_info()[2]
    assert_is(a_return_value, asynq.debug.format_tb(traceback_to_verify))
    mock_extract_tb.assert_called_once_with(traceback_to_verify)
    mock_format_list.assert_called_once_with(a_return_value)


@asynq.mock.patch('asynq.debug.format_error')
def test_asynq_stack_trace_formatter(mock_format_error):
    mock_format_error.return_value = u'This is some traceback.'
    stderr_string_io = StringIO()
    handler = logging.StreamHandler(stream=stderr_string_io)
    handler.setFormatter(asynq.debug.AsynqStackTracebackFormatter())
    logger = logging.getLogger('test_asynq')
    logger.addHandler(handler)
    exc_info = None
    try:
        async_function_whose_child_async_task_will_throw_an_error()
    except ValueError:
        exc_info = sys.exc_info()
        logger.exception('Test')
    ty, val, tb = exc_info
    mock_format_error.assert_called_once_with(val, tb=tb)
    assert_eq(u'Test\nThis is some traceback.\n', stderr_string_io.getvalue())
