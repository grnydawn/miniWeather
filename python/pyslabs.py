import os, pickle, shutil, time, uuid

import slabif


_CONFIG_FILE = "__config__"
_FINISHED = "__finished__"
_MAX_OPEN_WAIT = 10 # seconds
_MAX_CLOSE_WAIT = 100 # seconds
_CONFIG_INIT = {
    "version": 1,
    "dims": {},
    "vars": {},
    "attrs": {},
    "__control__": {
        "master": None
    }
} 


class VariableWriter():

    def __init__(self, path, config):

        self.path = path
        self.config = config
        self.writecount = 0

    def write(self, slab, start):

        path = self.path

        try:
            for _s in start:
                path = os.path.join(path, str(_s))
                if not os.path.isdir(path):
                    os.makedirs(path)

        except TypeError:
            path = os.path.join(str(start))
            os.makedirs(path)

        wc = str(self.writecount)
        atype, ext = slabif.arraytype(slab)
        slabpath = os.path.join(path, ".".join([wc, atype, ext])) 

        with open(slabpath, "wb") as fp:
            try:
                slabif.dump(slab, fp)

            except Exception as err:
                pickle.dump(slab, fp)

            fp.flush()
            os.fsync(fp.fileno())

        self.writecount += 1


class VariableReader():

    def __init__(self, path, config):

        self.path = path
        self.config = config

    def get_array(self):
        import pdb; pdb.set_trace()
        

class MasterPyslabsWriter():

    def __init__(self, root):
        self.root = root

    def begin(self):
 
        self.config["__control__"]["master"] = {self.uuid: None}

        with open(self.configpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())
       
    def define_var(self, name):

        varcfg = {}
        self.config["vars"][name] = varcfg

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):
        pass


class ParallelPyslabsWriter():

    def __init__(self, root):
        self.root = root
        self.uuid = str(uuid.uuid4().hex)
        self.path = os.path.join(self.root, self.uuid)
        self.cfgpath = os.path.join(self.root, _CONFIG_FILE)

        with open(self.cfgpath, "rb") as fp:
            self.config = pickle.load(fp)

    def get_var(self, name):

        varcfg = self.config["vars"][name]

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):

        # notify master that it is finished
        with open(os.path.join(self.path, _FINISHED), "w") as fp:
            fp.write("DONE")
            fp.flush()
            os.fsync(fp.fileno())


class MasterPyslabsWriter(ParallelPyslabsWriter):

    def begin(self):
 
        self.config["__control__"]["master"] = {self.uuid: None}

        with open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())
       
    def define_var(self, name):

        varcfg = {}
        self.config["vars"][name] = varcfg

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):
 
        def _move_dim(src, dst):
            
            for dim in os.listdir(src):
                srcpath = os.path.join(src, dim)
                dstpath = os.path.join(dst, dim)

                if os.path.isdir(srcpath):
                    if os.path.isdir(dstpath):
                        _move_dim(srcpath, dstpath) 

                    elif os.path.exists(dstpath):
                        raise Exception("Destination path already exists: %s" % dstpath)

                    else:
                        shutil.move(srcpath, dstpath)

                elif os.path.exists(dstpath):
                    raise Exception("Multiple processes creat the same data file: %s" % dstpath)

                else:
                    shutil.move(srcpath, dstpath)
              
        def _move_proc(src, dst):

            for var in os.listdir(src): 
                dstvar = os.path.join(dst, var)
                srcvar = os.path.join(src, var)

                if not os.path.isdir(dstvar):
                    shutil.move(srcvar, dstvar)

                else:
                    _move_dim(srcvar, dstvar) 

        procs = []

        start = time.time()
        for item in os.listdir(self.root):
            if item == self.uuid:
                procs.append(os.path.join(self.root, item))
                continue

            try:
                if len(item) == len(self.uuid) and int(item, 16):
                    proc = os.path.join(self.root, item)
                    procs.append(proc)
                    finished = os.path.join(proc, _FINISHED)
                    timeout = True

                    while time.time() - start < _MAX_CLOSE_WAIT:
                        if os.path.isfile(finished):
                            os.remove(finished)
                            timeout = False
                            break
                        time.sleep(0.1)

                    if timeout:
                        raise Exception("Error: timeout on waiting for parallel process finish.")

            except ValueError:
                pass

        # restructure data folders
        for src in procs:
            _move_proc(src, self.root)
            shutil.rmtree(src)

        self.config["__control__"]["master"] = None

        with open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())
#
#        # archive if requested
#        if self.archive:
#            dirname, basename = os.path.split(self.root)
#            arcpath = os.path.join(dirname, basename+_EXT)
#
#            with tarfile.open(arcpath, "w") as tar:
#                tar.add(self.root, arcname=basename)
#
#            #shutil.rmtree(self.root)

        # TODO: coordinate with slaves removing output paths


class ParallelPyslabsReader():

    def __init__(self, root):
        self.root = root
        self.cfgpath = os.path.join(self.root, _CONFIG_FILE)

        with open(self.cfgpath, "rb") as fp:
            self.config = pickle.load(fp)

    def get_array(self, name):

        varcfg = self.config["vars"][name]

        var = VariableReader(os.path.join(self.root, name), varcfg)

        try:
            return slabif.get_array(var)

        except Exception as err:
            return var.get_array() 

    def close(self):

        pass

    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        pass


class MasterPyslabsReader(ParallelPyslabsReader):
    pass


def master_open(path, mode="r"):

    cfgpath = os.path.join(path, _CONFIG_FILE)

    if mode == "w":

        # create root directory
        os.makedirs(path, exist_ok=False)

        # create a config file
        with open(cfgpath, "wb") as fp:
            pickle.dump(_CONFIG_INIT, fp)
            fp.flush()
            os.fsync(fp.fileno())

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)

    if not os.path.isfile(cfgpath):
        raise Exception("Target configuration does not exist: %s" % cfgpath)

    if mode[0] == "w":
        return MasterPyslabsWriter(path)

    elif mode[0] == "r":
        return MasterPyslabsReader(path)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))


def parallel_open(path, mode="r"):

    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isdir(path):
            break
        time.sleep(0.1)

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)
 
    cfgpath = os.path.join(path, _CONFIG_FILE)

    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isfile(cfgpath):
            break
        time.sleep(0.1)

    if not os.path.isfile(cfgpath):
        raise Exception("Target configuration does not exist: %s" % cfgpath)
 
    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        with open(cfgpath, "rb") as fp:
            cfg = pickle.load(fp)
            if cfg["__control__"]["master"] is None:
                time.sleep(0.1)
                continue
            if mode[0] == "w":
                return ParallelPyslabsWriter(path)

            elif mode[0] == "r":
                return ParallelPyslabsReader(path)

            else:
                raise Exception("Unknown open mode: %s" % str(mode))

    raise Exception("Target configuration is not configured: %s" % cfgpath)
