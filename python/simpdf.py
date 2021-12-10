import os, io, time, uuid, json, pickle, numpy, shutil, tarfile

_EXT = ".spdf"   
_ZIPEXT = ".spzf"   
_FINISHED = "__finished__"
_MAX_OPEN_WAIT = 10 # seconds
_MAX_CLOSE_WAIT = 100 # seconds
_CONFIG_FILE = "__config__"
_CONFIG_INIT = {
    "version": 1,
    "dims": {},
    "orders": {},
    "vars": {},
    "attrs": {},
    "__control__": {
        "master": None
    }
}

UNLIMITED = -1


class DimensionWriter():

    def __init__(self, name, conf):
        self.name = name
        self.conf = conf


class OrderWriter(DimensionWriter):
    pass

class VariableWriter():

    def __init__(self, path, name, conf):

        self.path = path
        self.name = name
        self.conf = conf
        self.write_count = 0
        self.varpath = os.path.join(path, name)

        if not os.path.isdir(self.varpath):
            os.makedirs(self.varpath)

    def path_encode(self, path):
        return path

    def write(self, var, start, order=None):

        if isinstance(start, (list, tuple)):
            _start = [str(s) for s in start]

        else:
            _start = [str(start)]

        path = self.varpath

        for s in _start:
            path = os.path.join(path, s)
            if not os.path.isdir(path):
                os.makedirs(path)

        filename = str(self.write_count) + "__"

        if isinstance(order, (list, tuple)):
            filename += "_".join([str(s) for s in order])

        else:
            filename += str(order)

        self.write_count += 1

        with io.open(os.path.join(path, self.path_encode(filename)), "wb") as fp:
            pickle.dump(var, fp)
            fp.flush()
            os.fsync(fp.fileno())


class SimpleParallelDataFormatWriter():

    def __init__(self, path, mode, config, archive):

        self.root = path
        self.uuid = str(uuid.uuid4().hex)
        self.path = os.path.join(self.root, self.uuid)
        self.mode = mode
        self.configpath = config
        self.archive = archive

        with io.open(self.configpath, "r") as fp:
            self.config = json.load(fp)

        if mode == "r":
            os.makedirs(self.path)

    def get_dimdef(self, name):
        return DimensionWriter(name, self.config["dims"][name])

    def get_vardef(self, name):
        return VariableWriter(self.path, name, self.config["vars"][name])

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
            json.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())
       
    def define_dim(self, name, size, desc):

        dim = {"size": size, "desc": desc}
        self.config["dims"][name] = dim

        return DimensionWriter(name, dim)

    def define_order(self, name, size, desc):

        order = {"size": size, "desc": desc}
        self.config["orders"][name] = order

        return OrderWriter(name, order)

    def define_var(self, name, shape, order=None, desc=None):

        if isinstance(order, (list, tuple)):
            _orders = [o.name for o in order]

        elif order is None:
            _orders = None

        else:
            _orders = [order.name]


        _shape = [s.name for s in shape]

        var = {"shape": _shape, "orders": _orders, "desc": desc}
        self.config["vars"][name] = var

        return VariableWriter(self.path, name, var)

    def close(self):

        def _move_var(src, dst):
            
            for dim in os.listdir(src):
                srcdim = os.path.join(src, dim)

                if os.path.isdir(srcdim):
                    dstdim = os.path.join(dst, dim)

                    if not os.path.isdir(dstdim):
                        shutil.move(srcdim, dstdim)

                    else:
                        _move_var(srcdim, dstdim) 
               
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
            json.dump(self.config, fp)
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


def master_open(path, mode="r", archive=True, exist_ok=False):

    config = os.path.join(path, _CONFIG_FILE)

    if mode == "w":
        os.makedirs(path, exist_ok=exist_ok)

        for item in os.listdir(path):
            shutil.rmtree(os.path.join(path, item))

        with io.open(config, "w") as fp:
            json.dump(_CONFIG_INIT, fp)
            fp.flush()
            os.fsync(fp.fileno())

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)

    if not os.path.isfile(config):
        raise Exception("Target configuration does not exist: %s" % config)

    if mode == "w":
        return MasterSimpleParallelDataFormatWriter(path, mode, config, archive)

    elif mode == "r":
        return MasterSimpleParallelDataFormatReader(path, mode, config, archive)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))


def open(path, mode="r", archive=True):

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
            cfg = json.load(fp)
            if cfg["__control__"]["master"] is None:
                time.sleep(0.1)
                continue
            if mode == "w":
                return SimpleParallelDataFormatWriter(path, mode, config, archive)

            elif mode == "r":
                return SimpleParallelDataFormatReader(path, mode, config, archive)

            else:
                raise Exception("Unknown open mode: %s" % str(mode))

    raise Exception("Target configuration is not configured: %s" % config)
