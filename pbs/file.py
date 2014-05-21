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
    """ Class to track file changes 
        
        if (the two files' name, size, and mtime are the same):
            assume equivalent
        elif (the two files' sha256 hash is the same):
            assume equivalent
        else:
            assume inequivalent
            
    """
    def __init__(self, name=None, size=None, mtime=None, sha256=None):
        self.name = os.path.abspath(name)
        self.size = size
        self.mtime = mtime
        self.sha256 = None
    
    
    def __eq__(self, f):
        if self.name == f.name:
            if self.size == f.size and self.mtime == f.mtime :
                return True
            else:
                if self.sha256 == None:
                    self.hash()
                if f.sha256 == None:
                    f.hash()
                if self.sha256 == f.sha256:
                    return True
        return False
    
    
    def startpoint(self):
        self.hash()
        self.stat()
    
    
    def endpoint(self):
        self.stat()
    
    
    def hash(self):
        self.sha256 = hash_sha256(self.name)
    
    
    def stat(self):
        self.size = os.path.getsize(self.name)
        self.mtime = os.path.getmtime(self.name)
    
    
    def serialize(self):
        return json.dumps(self.__dict__)
    
    @staticmethod
    def deserialize(json_str):
        return File(**json.loads(json_str))

    


class FileList(object):
    def __init__(self, filelist=None):
        if filelist == None:
            self.filelist = []
        elif isinstance(filelist, FileList):
            self.filelist = list(filelist.filelist)
        elif isinstance(filelist, list):
            self.filelist = filelist
            for i in range(len(self.filelist)):
                if isinstance(self.filelist[i], File):
                    pass
                elif isinstance(self.filelist[i], str):
                    self.filelist[i] = File(name = self.filelist[i])
                else:
                    raise PBSError("Can not initialize FileList. 'filelist' is a list of invalid type " + f.__class__.__name__)
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
    
    
    def startpoint(self):
        for file in self.filelist:
            file.startpoint()
    
    
    def endpoint(self):
        for file in self.filelist:
            file.endpoint()
    
    
    def serialize(self):
        return json.dumps({"filelist":[f.__dict__ for f in self.filelist]})
    
    
    @staticmethod
    def deserialize(json_str):
        return FileList([File(**f) for f in json.loads(json_str)['filelist'] ])
    
    
    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return self.serialize()


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
    
    @staticmethod
    def deserialize(json_str):
        d = json.loads(json_str)
        d['filelist'] = FileList.deserialize( d['filelist'])
        return WatchDir(**d)
    
    
    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return self.serialize()
    
    
    def startpoint(self):
        self.setpoint(start = True)
    
    
    def endpoint(self):
        self.setpoint(start = False)
    
    
    def setpoint(self, start=False):
        """ Look through the directory (and subdirectories if recursive==True),
            and save a filelist containing all files.
            
            If 'start' == True, also save the files' hash values.
        """
        self.filelist = FileList()
        if self.recursive:
            for root, dirs, files in os.walk(self.name):
                for filename in files:
                    filepath = os.path.abspath(os.path.join(root,filename))
                    file = File(filepath)
                    if start == True:
                        file.hash()
                    self.filelist.append(file)
        else:
            for item in os.listdir(self.name):
                filepath = os.path.abspath( os.path.join(self.name, item))
                if os.path.isfile( filepath):
                    file = File(filepath)
                    if start == True:
                        file.hash()
                    self.filelist.append(file)
                
    
    def diff(self):
        """ Returns a filelist containing new and modified files in the watched path """
        current = WatchDir(self.name, self.recursive)
        current.endpoint()
        return FileList([f for f in current.filelist if (f not in self.filelist)])


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
    
    
    def startpoint(self):
        for d in self.dirlist():
            d.startpoint()
    
    
    def endpoint(self):
        for d in self.dirlist():
            d.endpoint()
    
    
    def serialize(self):
        return json.dumps({"dirlist":[d.serialize() for d in self.dirlist]})
    
    
    @staticmethod
    def deserialize(json_str):
        wdl = json.loads(json_str)
        wdl["dirlist"] = [ WatchDir.deserialize(wd) for wd in wdl["dirlist"] ]
        return WatchDirList(**wdl)
    
    
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

