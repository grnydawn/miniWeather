import os, io, time, uuid, pickle, numpy, shutil, tarfile

_EXT = ".spdf"   
_ZIPEXT = ".spzf"   
_FINISHED = "__finished__"
__LABEL__ = "__label__"
__STREAM__ = "stream"
__LABELINIT__ = "__labelinit__"
_MAX_OPEN_WAIT = 10 # seconds
_MAX_CLOSE_WAIT = 100 # seconds
_CONFIG_FILE = "__config__"
_CONFIG_INIT = {
    "version": 1,
    "dims": {},
    "vars": {},
    "attrs": {},
    "__control__": {
        "master": None
    }
}

UNLIMITED = -1


class Dimension():

    def __init__(self, name, conf):
        self.name = name
        self.conf = conf


class Stream(Dimension):

    def __init__(self, name, conf):
        super(Stream, self).__init__(self, name, conf)
        self._count = 0


class Variable():

    def __init__(self, name, path, conf, dims):

        self.name = name
        self.path = path
        self.conf = conf
        self.dims = dims
        self.streams = dict((k,v) for k, v in dims.items() if v["is_stream"])
        self._count = 0

        if not os.path.isdir(self.path):
            os.makedirs(self.path)

    def _intstr(s):
        return str(s(self._count) if callable(s) else int(s))

    def write(self, var, start, stream=None, shape=None, datatype=None, strict=False, **kwargs):
        """
            var: data to be saved
            start: staring indices
            shape: size check of each dimensions
            datatype: data type check
            strict: check all elements?
"""

        if isinstance(start, (list, tuple)):
            _start = [self._intstr(s)) for s in start]

        else:
            _start = [self._intstr(start)]

        path = self.path

        for s in _start:
            path = os.path.join(path, s)
            if not os.path.isdir(path):
                os.makedirs(path)

        # TODO: shape checking
        # TODO: data type checking

        countstr = str(self._count)

        if stream:
            if self.default_stream:
                streamname = str(self.default_stream["count"])
                self.default_stream["count"] += 1

            
        for name in self.conf["shape"]:
            labelfile = os.path.join(dims[name]["labeldir"], orderstr)

            if not os.path.isfile(labelfile):
                with io.open(labelfile, "wb") as fp:
                    pickle.dump(stream, fp)
                    fp.flush()
                    os.fsync(fp.fileno())

        with io.open(os.path.join(path, countstr), "wb") as fp:
            pickle.dump(var, fp)
            fp.flush()
            os.fsync(fp.fileno())

        self._count += 1

#class VariableReader():
#
#    def __init__(self, name, path, conf, dims, orders):
#
#        self.name = name
#        self.path = path
#        self.conf = conf
#        self.dims = dims
#        self.orders = orders
#
#
#    def __iter__(self):
#        return self
# 
#    def __next__(self):
#        import pdb; pdb.set_trace()
#        pass
#  
#    def __len__(self):
#        import pdb; pdb.set_trace()
#        pass
#
#    def __contains__(self):
#        import pdb; pdb.set_trace()
#
#    def __getitem__(self, key):
#        import pdb; pdb.set_trace()
#        pass

class SimpleParallelDataFormatWriter():

    def __init__(self, path, mode, config, archive):

        self.root = path
        self.uuid = str(uuid.uuid4().hex)
        self.path = os.path.join(self.root, self.uuid)
        self.mode = mode
        self.configpath = config
        self.archive = archive

        with io.open(self.configpath, "r") as fp:
            self.config = pickle.load(fp)

        if mode == "r":
            os.makedirs(self.path)

    def get_dimdef(self, name):
        return Dimension(name, self.config["dims"][name])

    def get_var(self, name):

        return Variable(name, os.path.join(self.path, name),
                            self.config["vars"][name], self.config["dims"])

    def close(self):

        # notify master that it is finished
        with io.open(os.path.join(self.path, _FINISHED), "w") as fp:
            fp.write("DONE")
            fp.flush()
            os.fsync(fp.fileno())



class MasterSimpleParallelDataFormatWriter(SimpleParallelDataFormatWriter):

    def begin(self):
 
        self.config["__control__"]["master"] = {self.uuid: None}

        with io.open(self.configpath, "w") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())
       
    def _create_labeldir(self, name, label)

        labelroot = os.path.join(self.root, __LABEL__)
        labeldir = os.path.join(labelroot, name)

        if not os.path.isdir(labeldir):
            os.makedirs(labeldir)

        if label:
            labelobj = os.path.join(labeldir, __LABELINIT__)
            with open(labelinit, "wb") as fp:
                pickle.dump(label, fp)
                fp.flush()
                os.fsync(fp.fileno())

        return labeldir

    def define_dim(self, name, length=None, desc=None, label=None):

        if name in self.config["dims"]:
            raise Exception("Name '%s' already exists." % name)

        labeldir = self._create_labeldir(name, label)

        dim = {"length": length, "desc": desc, "labeldir"=labeldir,
                "is_stream": False}
        self.config["dims"][name] = dim

        return Dimension(name, dim)

    def define_stream(self, name, length=None, desc=None, label=None, default=None):

        self.define_dim(name, length=length, desc=desc, label=label)

        stream = self.config["dims"][name]
        stream["is_stream"] = True
        stream["default_stream"] = default

        return Stream(name, stream)

    def define_var(self, name, shape=None, stream=None, datatype=None, strict=False, desc=None):
        """
            name: variable name
            shape: total length of each dimensions, including stream
            datatype: force to check data type
            strict: check all elements
            desc: variable description
    """
        # stream should be the last part of shape
        # stream can be multiple

        if shape:
            shape = [s.name for s in shape]

        if stream:
            stream = [s.name for s in stream]

        var = {"shape": shape, "stream": stream, "datatype": datatype,
                "strict": strict, "desc": desc}
        self.config["vars"][name] = var

        return Variable(name, os.path.join(self.path, name), var,
                self.config["dims"])

    def close(self):

        # TODO: unlimited order should be converted a number

        def _move_var(src, dst):
            
            for dim in os.listdir(src):
                srcdim = os.path.join(src, dim)

                if os.path.isdir(srcdim):
                    dstdim = os.path.join(dst, dim)

                    if os.path.isdir(dstdim):
                        _move_var(srcdim, dstdim) 

                    else:
                        shutil.move(srcdim, dstdim)
               
        def _move_proc(src, dst):

            for var in os.listdir(src): 
                dstvar = os.path.join(dst, var)
                srcvar = os.path.join(src, var)

                if not os.path.isdir(dstvar):
                    shutil.move(srcvar, dstvar)

                else:
                    _move_var(srcvar, dstvar) 

        start = time.time()
        for item in os.listdir(self.root):
            if item == self.uuid:
                continue

            try:
                if len(item) == 32 and int(item, 16):
                    finished = os.path.join(self.root, item, _FINISHED)

                    while time.time() - start < _MAX_CLOSE_WAIT:
                        if os.path.isfile(finished):
                            os.remove(finished)
                            break
                        time.sleep(0.1)

            except ValueError:
                pass

        # restructure data folders
        for item in os.listdir(self.root):
            try:
                if len(item) == 32 and int(item, 16):
                    src = os.path.join(self.root, item)
                    _move_proc(src, self.root)
                    shutil.rmtree(src)

            except ValueError:
                pass

        self.config["__control__"]["master"] = None

        with io.open(self.configpath, "w") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())

        # archive if requested
        if self.archive:
            dirname, basename = os.path.split(self.root)
            arcpath = os.path.join(dirname, basename+_EXT)

            with tarfile.open(arcpath, "w") as tar:
                tar.add(self.root, arcname=basename)

            #shutil.rmtree(self.root)

        # TODO: coordinate with slaves removing output paths


#class MasterSimpleParallelDataFormatReader(SimpleParallelDataFormatReader):
#
#    def __init__(self, path, mode, config):
#
#        self.path = path
#        self.mode = mode
#        self.configpath = config
#
#        with io.open(self.configpath, "r") as fp:
#            self.config = json.load(fp)
#
#    def __enter__(self):
#
#        return self
#
#    def __exit__(self, type, value, traceback):
#
#        pass
#
#    def get_var(self, name):
#        path = os.path.join(self.path, name)
#        conf = self.config["vars"][name]
#        dims = dict((d, self.config["dims"][d]) for d in conf["shape"])
#        orders = dict((o, self.config["orders"][o]) for o in conf["orders"])
#        return VariableReader(name, path, conf, dims, orders)


def master_open(path, mode="r", archive=True, exist_ok=False):

    config = os.path.join(path, _CONFIG_FILE)

    if mode == "w":

        # create root directory
        os.makedirs(path, exist_ok=exist_ok)

        # remove all remains
        for item in os.listdir(path):
            shutil.rmtree(os.path.join(path, item))

        # create a config file
        with io.open(config, "w") as fp:
            pickle.dump(_CONFIG_INIT, fp)
            fp.flush()
            os.fsync(fp.fileno())

        # create a label directory
        os.makedirs(os.path.join(path, __LABEL__))

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)

    if not os.path.isfile(config):
        raise Exception("Target configuration does not exist: %s" % config)

    if mode[0] == "w":
        return MasterSimpleParallelDataFormatWriter(path, mode, config, archive)

    elif mode[0] == "r":
        return MasterSimpleParallelDataFormatReader(path, mode, config)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))


def parallel_open(path, mode="r", archive=True):

    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isdir(path):
            break
        time.sleep(0.1)

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)
 
    start = time.time()
    config = os.path.join(path, _CONFIG_FILE)
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isfile(config):
            break
        time.sleep(0.1)

    if not os.path.isfile(config):
        raise Exception("Target configuration does not exist: %s" % config)
 
    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        with io.open(config, "r") as fp:
            cfg = pickle.load(fp)
            if cfg["__control__"]["master"] is None:
                time.sleep(0.1)
                continue
            if mode[0] == "w":
                return SimpleParallelDataFormatWriter(path, mode, config, archive)

            elif mode[0] == "r":
                return SimpleParallelDataFormatReader(path, mode, config, archive)

            else:
                raise Exception("Unknown open mode: %s" % str(mode))

    raise Exception("Target configuration is not configured: %s" % config)
