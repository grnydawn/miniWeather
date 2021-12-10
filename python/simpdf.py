import os, io, time, uuid, json, pickle, numpy

_EXT = ".spdf"   
_MAX_WAIT = 10
_CONFIG_FILE = "config"
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

class Dimension():

    def __init__(self, name, conf):
        self.name = name
        self.conf = conf


class Order(Dimension):
    pass

class Variable():

    def __init__(self, path, name, conf):

        self.path = path
        self.name = name
        self.conf = conf
        self.write_count = 0
        self.varpath = os.path.join(path, name)

        if not os.path.isdir(self.varpath):
            os.makedirs(self.varpath)

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

        if order is None:
            filename = str(self.write_count)

        elif isinstance(order, (list, tuple)):
            filename = "_".join([str(s) for s in order])

        else:
            filename = str(order)

        self.write_count += 1

        with io.open(os.path.join(path, filename), "wb") as fp:
            pickle.dump(var, fp)


class SimpleParallelDataFormat():

    def __init__(self, path, mode, config, archive):

        self.root = path
        self.uuid = str(uuid.uuid4())
        self.path = os.path.join(self.root, self.uuid)
        self.mode = mode
        self.config = config
        self.archive = archive

        os.makedirs(self.path)

    def get_dimdef(self, name):

        with io.open(self.config, "r") as fp:
            cfg = json.load(fp)
            dim = cfg["dims"][name]
            return Dimension(name, dim)

    def get_vardef(self, name):

        with io.open(self.config, "r") as fp:
            cfg = json.load(fp)
            var = cfg["vars"][name]
            return Variable(self.path, name, var)

    def close(self):
        pass
        # may coordinate with master


class MasterSimpleParallelDataFormat(SimpleParallelDataFormat):

    def begin(self):
 
        with io.open(self.config, "r+") as fp:
            cfg = json.load(fp)
            cfg["__control__"]["master"] = {self.uuid: None}
            fp.seek(0)
            json.dump(cfg, fp)
            os.fsync(fp.fileno())
       
    def define_dim(self, name, size, desc):

        dim = {"size": size, "desc": desc}

        with io.open(self.config, "r+") as fp:
            cfg = json.load(fp)
            cfg["dims"][name] = dim
            fp.seek(0)
            json.dump(cfg, fp)
            os.fsync(fp.fileno())

        return Dimension(name, dim)

    def define_order(self, name, size, desc):

        order = {"size": size, "desc": desc}

        with io.open(self.config, "r+") as fp:
            cfg = json.load(fp)
            cfg["orders"][name] = order
            fp.seek(0)
            json.dump(cfg, fp)
            os.fsync(fp.fileno())

        return Order(name, order)

    def define_var(self, name, shape, order=None, desc=None):

        if isinstance(order, (list, tuple)):
            _orders = [o.name for o in order]

        elif order is None:
            _orders = None

        else:
            _orders = [order.name]


        _shape = [s.name for s in shape]

        var = {"shape": _shape, "orders": _orders, "desc": desc}

        with io.open(self.config, "r+") as fp:
            cfg = json.load(fp)
            cfg["vars"][name] = var
            fp.seek(0)
            json.dump(cfg, fp)
            os.fsync(fp.fileno())

        return Variable(self.path, name, var)

    def close(self):

        # restructure data folders

        # archive if requested
        if self.archive:
            pass
 
        with io.open(self.config, "r") as fp:
            cfg = json.load(fp)

        cfg["__control__"]["master"] = None

        with io.open(self.config, "w") as fp:
            json.dump(cfg, fp)
            os.fsync(fp.fileno())


def master_open(path, mode="r", archive=True, exist_ok=False):

    config = os.path.join(path, _CONFIG_FILE)

    if mode[0] == "w":
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

    return MasterSimpleParallelDataFormat(path, mode, config, archive)

def open(path, mode="r", archive=True):

    start = time.time()
    while time.time() - start < _MAX_WAIT:
        if os.path.isdir(path):
            break
        time.sleep(0.1)

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)
 
    start = time.time()
    config = os.path.join(path, _CONFIG_FILE)
    while time.time() - start < _MAX_WAIT:
        if os.path.isfile(config):
            break
        time.sleep(0.1)

    if not os.path.isfile(config):
        raise Exception("Target configuration does not exist: %s" % config)
 
    start = time.time()
    while time.time() - start < _MAX_WAIT:
        with io.open(config, "r") as fp:
            cfg = json.load(fp)
            if cfg["__control__"]["master"] is None:
                time.sleep(0.1)
                continue
            return SimpleParallelDataFormat(path, mode, config, archive)

    raise Exception("Target configuration is not configured: %s" % config)
