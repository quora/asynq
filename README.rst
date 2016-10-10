.. image:: http://i.imgur.com/jCPNyOa.png

.. image:: https://travis-ci.org/quora/asynq.svg?branch=master
    :target: https://travis-ci.org/quora/asynq

``asynq`` is a library for asynchronous programming in Python with a focus on batching requests to
external services. It also provides seamless interoperability with synchronous code, support for
asynchronous context managers, and tools to make writing and testing asynchronous code easier.
``asynq`` was developed at Quora and is a core component of Quora's architecture. See the original blog
post `here <https://engineering.quora.com/Asynchronous-Programming-in-Python>`_.

The most important use case for ``asynq`` is batching. For many storage services (e.g., memcache,
redis) it is far faster to make a single request that fetches many keys at once than to make
many requests that each fetch a single key. The ``asynq`` framework makes it easy to write code
that takes advantage of batching without radical changes in code structure from code that does not
use batching.

For example, synchronous code to retrieve the names of the authors of a list of Quora answers may
look like this:

.. code-block:: python

    def all_author_names(aids):
        uids = [author_of_answer(aid) for aid in aids]
        names = [name_of_user(uid) for uid in uids]
        return names

Here, each call to ``author_of_answer`` and ``name_of_user`` would result in a memcache request.
Converted to use ``asynq``, this code would look like:

.. code-block:: python

    @async()
    def all_author_names(aids):
        uids = yield [author_of_answer.async(aid) for aid in aids]
        names = yield [name_of_user.async(uid) for uid in uids]
        result(names); return

All ``author_of_answer`` calls will be combined into a single memcache request, as will all of the
``name_of_user`` calls.

Futures
-------

Futures are the basic building blocks of ``asynq``'s programming model. The scheduler keeps track
of futures and attempts to schedule them in an efficient way. ``asynq`` uses its own hierarchy of
Future classes, rooted in ``asynq.FutureBase``. Futures have a ``.value()`` method that computes
their value if necessary and then returns it.

The following are the most important Future classes used in ``asynq``:

- ``AsyncTask``, a Future representing the execution of an asynchronous function (see below).
  Normally created by calling ``.async()`` on an asynchronous function.
- ``ConstFuture``, a Future whose value is known at creation time. This is useful when you need
  to pass a Future somewhere, but no computation is actually needed.
- ``BatchBase`` and ``BatchItemBase``, the building blocks for doing batching. See below for
  details.


Decorators and asynchronous functions
-------------------------------------

``asynq``'s asynchronous functions are implemented as Python generator functions. Every time an
asynchronous functions yields one or more Futures, it cedes control the asynq scheduler, which will
resolve the futures that were yielded and continue running the function after the futures have been
computed.

Returning a value from an asynchronous function is hard in Python 2 because generators are not
allowed to return a value. ``asynq`` provides the special ``result()`` function that can be used to
return a value from an asynchronous function; it is implemented by raising a custom exception
that is caught by the scheduler. At Quora, we instead use a patched Python 2 binary that does
support returning a value from a generator.

The framework requires usage of the ``@async()`` decorator on all asynchronous functions. This
decorator wraps the generator function so that it can be called like a normal, synchronous function.
It also creates a ``.async`` attribute on the function that allows calling the function
asynchronously. Calling this attribute will return an ``AsyncTask`` object corresponding to the
function.

You can call an asynchronous function synchronously like this:

.. code-block:: python

    result = async_fn(a, b)

and asynchronously like this:

.. code-block:: python

    result = yield async_fn.async(a, b)

Calling ``async_fn.async(a, b).value()`` has the same result as ``async_fn(a, b)``.

The decorator has a ``pure=True`` option that disables the ``.async`` attribute and instead makes
the function itself asynchronous, so that calling it returns an ``AsyncTask``. We recommend to use
this option only in special cases like decorators for asynchronous functions.

``asynq`` also provides an ``@async_proxy()`` decorator for functions that return a Future
directly. Functions decorated with ``@async_proxy()`` look like ``@async()`` functions externally.
An example use case is a function that takes either an asynchronous or a synchronous function,
and calls it accordingly:

.. code-block:: python

    @async_proxy()
    def async_call(fn, *args, **kwargs):
        if is_async_fn(fn):
            # Returns an AsyncTask
            return fn.async(*args, **kwargs)
        return ConstFuture(fn(*args, **kwargs))

Batching
--------

Batching is at the core of what makes ``asynq`` useful. To implement batching, you need to subclass
``asynq.BatchItemBase`` and ``asynq.BatchBase``. The first represents a single entry in a batch
(e.g., a single memcache key to fetch) and the second is responsible for executing the batch when
the scheduler requests it.

Batch items usually do not require much logic beyond registering themselves with the currently
active batch in ``__init__``. Batches need to override the ``_try_switch_active_batch`` method,
which changes the batch that is currently active, and the ``_flush`` method that executes it.
This method should call ``.set_value()`` on all the items in the batch.

An example implementation of batching for memcache is in the ``asynq/examples/batching.py`` file.
The framework also provides a ``DebugBatchItem`` for testing.

Most users of ``asynq`` should not need to implement batches frequently. At Quora, we use
thousands of asynchronous functions, but only five ``BatchBase`` subclasses.

Contexts
--------

``asynq`` provides support for Python context managers that are automatically activated and
deactivated when a particular task is scheduled. This feature is necessary because the scheduler
can schedule tasks in arbitrary order. For example, consider the following code:

.. code-block:: python

    @async()
    def show_warning():
        yield do_something_that_creates_a_warning.async()

    @async()
    def suppress_warning():
        with warnings.catch_warnings():
            yield show_warning.async()

    @async()
    def caller():
        yield show_warning.async(), suppress_warning.async()

This code should show only one warning, because only the second call to ``show_warning`` is within
a ``catch_warnings()`` context, but depending on how the scheduler happens to execute these
functions, the code that shows the warning may also be executed while ``catch_warnings()`` is
active.

To remedy this problem, you should use an ``AsyncContext``, which will be automatically paused when
the task that created it is no longer active and resumed when it becomes active again. An
``asynq``-compatible version of ``catch_warnings`` would look something like this:

.. code-block:: python

    class catch_warnings(asynq.AsyncContext):
        def pause(self):
            stop_catching_warnings()

        def resume(self):
            start_catching_warnings()

Debugging
---------

Because the ``asynq`` scheduler is invoked every time an asynchronous function is called, and it
can invoke arbitrary other active futures, normal Python stack traces become useless in a
sufficiently complicated application built on ``asynq``. To make debugging easier, the framework
provides the ability to generate a custom ``asynq`` stack trace, which shows how each active
asynchronous function was invoked.

The ``asynq.debug.dump_asynq_stack()`` method can be used to print this stack, similar to
``traceback.print_stack()``. The framework also registers a hook to print out the ``asynq`` stack
when an exception happens.

Tools
-----

``asynq`` provides a number of additional tools to make it easier to write asynchronous code. Some
of these are in the ``asynq.tools`` module. These tools include:

- ``asynq.async_call`` calls a function asynchronously only if it is asynchronous. This can be
  useful when calling an overridden method that is asynchronous on some child classes but not on others.
- ``asynq.tools.call_with_context`` calls an asynchronous function within the provided context
  manager. This is helpful in cases where you need to yield multiple tasks at once, but only one
  needs to be within the context.
- ``asynq.tools.afilter`` and ``asynq.tools.asorted`` are equivalents of the standard ``filter``
  and ``sorted`` functions that take asynchronous functions as their filter and compare functions.
- ``asynq.tools.acached_per_instance`` caches an asynchronous instance method.
- ``asynq.tools.deduplicate`` prevents multiple simultaneous calls to the same asynchronous
  function.
- The ``asynq.mock`` module is an enhancement to the standard ``mock`` module that makes it
  painless to mock asynchronous functions. Without this module, mocking any asynchronous function
  will often also require mocking its ``.async`` attribute. We recommend using ``asynq.mock.patch``
  for all mocking in projects that use ``asynq``.
- The ``asynq.generator`` module provides an experimental implementation of asynchronous
  generators, which can produce a sequence of values while also using ``asynq``'s batching support.

Compatibility
-------------

``asynq`` runs on Python 2.7 and Python 3.

Contributors
------------

`Alex Yakunin <https://github.com/alexyakunin>`_, `Jelle Zijlstra <https://github.com/JelleZijlstra>`_, `Manan Nayak <https://github.com/manannayak>`_, `Martin Michelsen <https://github.com/fuzziqersoftware>`_, `Shrey Banga <https://github.com/banga>`_, `Suren Nihalani <https://github.com/snihalani>`_ and
other engineers at Quora.
