import cython

cpdef inline bint is_asyncio_mode()

cdef class AsyncioMode:
    cpdef AsyncioMode __enter__(self)
    cpdef __exit__(self, exc_type, exc_value, tb)
