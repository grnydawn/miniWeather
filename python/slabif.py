import os

_supported_arrays = {
    "numpy": (lambda a: (type(a).__name__=="ndarray" and
                        type(a).__module__== "numpy"), "npy")
}


def arraytype(slab):
    for atype, (check, ext) in _supported_arrays.items():
        if check(slab):
            return atype, ext
        
    return "unknown", "dat"


def dump(slab, file):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        import numpy as np
        np.save(file, slab, allow_pickle=False)

    raise Exception("Not implemented: %s" % atype)


def stack(arrays, atype):

    if atype == "numpy":
        import numpy as np
        return np.stack(arrays)

    raise Exception("Not implemented: %s" % atype)


def load(path, atype):

    if atype == "numpy":
        import numpy as np
        slab = np.load(path, allow_pickle=False)
        return ("numpy", slab)

    raise Exception("Not implemented: %s" % atype)


def concat(bucket, array):

    if bucket[0] is None:
        atype = array[0]

    elif array[0] is None:
        atype = bucket[0]

    elif bucket[0] == array[0]:
        atype = bucket[0]

    else:
        import pdb; pdb.set_trace()

    if atype == "numpy":
        import numpy as np
        bucket[0] = atype

        if bucket[1] is None:
            bucket[1] = array[1]

        else:
            bucket[1] = np.concatenate((bucket[1], array[1]))

        return

    raise Exception("Unknown array type: %s" % array[0])


def _merge(path):

    _b = []
    _stack = []
    _atype = None

    for item in sorted(os.listdir(path)):
        _p = os.path.join(path, item)

        if os.path.isdir(_p):
            _b.append(_merge(_p))

        elif os.path.isfile(_p):
            _, atype, _ = item.split(".")

            if _atype is None:
                _atype = atype
                _stack.append(load(_p, atype)[1])

            elif _atype != atype:
                raise Exception("Different type exists in a stream: %s != %s" % (_atype, atype))

            else:
                _stack.append(load(_p, atype)[1])
            
        else:
            raise Exception("Unknown file type: %s" % _p)

    if _stack:
        _m = [_atype, stack(_stack, _atype)]

    else:
        _m = [None, None]

    for _i in _b:
        concat(_m, _i)
        
    return _m


def get_array(var):

    stype, arr = _merge(var.path)

    return arr
