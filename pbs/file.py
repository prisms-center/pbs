import os, time, json, hashlib, sqlite3, copy
import misc


def hash_sha256(name):
    h = hashlib.sha256()
    file = open(name,'r')
    for block in iter(lambda: file.read(16384), ""):
        h.update(block)
    file.close()
    return h.hexdigest()


class File(object):
    def __init__(self, name, sha256=None):
        self.name = os.path.abspath(name)
        self.sha256 = sha256
    
    
    def __eq__(self, f):
        if self.name == f.name and self.sha256 == f.sha256:
            return True
        return False
    
    
    def hash(self):
        self.sha256 = hash_sha256(self.name)
    
    
    def serialize(self):
        return json.dumps({'name':f.name,'sha256':f.sha256})


class FileList(object):
    def __init__(self, filelist=None):
        if filelist == None:
            self.filelist = []
        elif isinstance(filelist, FileList):
            self.filelist = list(filelist.filelist)
        elif isinstance(filelist, list):
            for f in filelist:
                if not isinstance(f, File):
                    raise PBSError("Can not initialize FileList. 'filelist' is a list of invalid type " + f.__class__.__name__)
            self.filelist = list(filelist)
        else:
            raise PBSError("Can not initialize FileList. 'filelist' is a list of invalid type " + f.__class__.__name__)
    
    
    def __iter__(self):
        for file in self.filelist:
            yield file
    
    
    def __len__(self):
        return len(self.filelist)
    
    
    def append(self, val):
        if isinstance(val, File):
            self.filelist.append(val)
        elif isinstance(val, FileList):
            self.filelist += val.filelist
        else:
            raise misc.PBSError("Can not append an object of type " + val.__class__.__name__ + "to a FileList.")
    
    
    def serialize(self):
        return json.dumps([f.__dict__() for f in self.filelist])
    
    
    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return json.dumps(self.serialize())


class WatchDir(object):
    def __init__(self, name, recursive = False, filelist = None):
        """ Args:
                name: path to directory to watch
                recursive: (bool) watch subdirectories?
                filelist: (FileList) list of files in directory, (initial files)
        """
        self.name = os.path.abspath(name)
        self.recursive = recursive
        if filelist == None:
            self.filelist = FileList()
        elif isinstance(filelist, FileList):
            self.filelist = copy.deepcopy(filelist)
        else:
            raise PBSError("Can not initialize WatchDir. 'filelist' is invalid type " + filelist.__class__.__name__)
    
    
    def serialize(self):
        return json.dumps({'name':self.name, 'recursive':self.recursive, 'filelist':self.filelist.serialize()})
    
    
    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return json.dumps(self.serialize())
    
    
    def hash(self):
        """ Look through the directory (and subdirectories if recursive==True),
            and save a filelist containing all files and their hash values.
        """
        self.filelist = FileList()
        if self.recursive:
            for root, dirs, files in os.walk(self.name):
                for filename in files:
                    filepath = os.path.abspath(os.path.join(root,filename))
                    file = File(filepath)
                    file.hash()
                    self.filelist.append(file)
        else:
            for item in os.listdir(self.name):
                filepath = os.path.abspath( os.path.join(self.name, item))
                if os.path.isfile( filepath):
                    file = File(filepath)
                    file.hash()
                    self.filelist.append(file)
                
    
    def diff(self):
        """ Returns a filelist containing new and modified files in the watched path """
        current = WatchDir(self.name, self.recursive)
        current.hash()
        return FileList([File(f.name, f.sha256) for f in current.filelist if (f not in self.filelist)])


class WatchDirList(object):
    def __init__(self, dirlist = None):
        """
            Args:
                dirlist = list of WatchDir objects
        """
        if dirlist == None:
            self.dirlist = list([])
        else:
            self.dirlist = copy.deepcopy(dirlist)
    
    
    def __iter__(self):
        for dir in self.dirlist:
            yield dir
    
    
    def __len__(self):
        return len(self.dirlist)
    
    
    def serialize(self):
        return json.dumps([f.serialize() for f in self.dirlist])
    
    
    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return json.dumps(self.serialize())
    
    
    def append(self, val):
        if isinstance(val, WatchDir):
            self.dirlist.append(val)
        elif isinstance(val, WatchDirList):
            self.dirlist += val.dirlist
        else:
            raise misc.PBSError("Can not append an object of type " + val.__class__.__name__ + "to a WatchDirList.")
    
    
    def diff(self):
        """ Returns a filelist containing new and modified files in all watched directories """
        total = FileList([])
        for dir in self.dirlist:
            total.append(dir.diff())
        return total

