import cython

cdef object _state

@cython.locals(out=list)
cpdef flush()
cpdef append(object stats)
cpdef reset()
cpdef incr_counter()
