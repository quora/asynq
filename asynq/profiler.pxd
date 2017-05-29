import cython

cdef object _stats

@cython.locals(out=list)
cpdef flush()
cpdef append(object stats)
cpdef reset()
