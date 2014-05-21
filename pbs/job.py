import subprocess, re, os, StringIO
import jobdb, misc

class Job(object):
    """A qsub Job object.
    
    Initialize either with all the parameters, or with 'qsubstr' a PBS submit script as a string.
    If 'qsubstr' is given, all other arguments are ignored and set using Job.read().
        
    
    Contains variables (with example values):
        name        "jobname"
        account     "prismsproject_flux"
        nodes       2
        ppn         16
        walltime    "10:00:00"
        pmem        "3800mb"
        queue       "flux"
        message     "abe"
        email       "jdoe@umich.edu"
        priority    "-200"
        command     "echo \"hello\" > test.txt"
        auto        True
        input               ["in1.txt", "path/to/in2.txt"]
        output              ["out1.txt", "path/to/out2.txt"]
        watchdir            ["dir1", "path/to/dir2"]
        watchdir_recurs     ["dir1", "path/to/dir2"]
        
        Only set to auto=True if the 'command' uses this pbs module to set itself as completed when it is completed.
           Otherwise, you may submit it extra times leading to wasted resources and overwritten data.
           
    
    """

    def __init__(self, name = "STDIN", \
                       account = None, \
                       nodes = None, \
                       ppn = None, \
                       walltime = None, \
                       pmem = None, \
                       queue = None, \
                       exetime = None, \
                       message = "a", \
                       email = None, \
                       priority = "0", \
                       command = None, \
                       auto = False, \
                       input = [], \
                       output = [], \
                       watchdir = [], \
                       watchdir_recurs = [], \
                       qsubstr = None):
        
        if qsubstr != None:
            self.read(qsubstr)
            return
        
        # Declares a name for the job. The name specified may be up to and including 
        # 15 characters in length. It must consist of printable, non white space characters 
        # with the first character alphabetic.
        # If the name option is not specified, to STDIN.
        self.name = name
        
        # account string
        self.account = account
        
        # number of nodes to request
        self.nodes = int(nodes)
        
        # number of processors per node to request
        self.ppn = int(ppn)
        
        # string walltime for job (HH:MM:SS) 
        self.walltime = walltime
        
        # string memory requested (1000mb)
        self.pmem = pmem
        
        # queue string
        self.queue = queue
        
        # time eligible for execution
        # PBS -a exetime
        # Declares the time after which the job is eligible for execution, 
        # where exetime has the form: [[[[CC]YY]MM]DD]hhmm[.SS]
        # create using pbs.misc.exetime( deltatime), where deltatime is a [[[DD:]MM:]HH:]SS string
        self.exetime = exetime
        
        # when to send email about the job
        # The mail_options argument is a string which consists of either the single 
        # character "n", or one or more of the characters "a", "b", and "e".
        #
        # If the character "n" is specified, no normal mail is sent. Mail for job 
        # cancels and other events outside of normal job processing are still sent.
        # 
        # For the letters "a", "b", and "e":
        # a     mail is sent when the job is aborted by the batch system.
        # b     mail is sent when the job begins execution.
        # e     mail is sent when the job terminates.
        self.message = message
        
        # User list to send email to. The email string is of the form:
        #       user[@host][,user[@host],...]
        self.email = email
        
        # Priority ranges from (low) -1024 to (high) 1023
        self.priority = priority
        
        # text string with command to run
        self.command = command
        
        # if True, simply rerun job until complete; if False, human intervention required
        # 'auto' jobs should set JobDB status to "finished" when finished
        self.auto = bool(auto)
        
        # list of files (with path) to save as input files
        self.input = input
        
        # list of files (with path) to save as output files
        self.output = output
        
        # list of directories (with path) to watch for changes to save as output files
        self.watchdir = watchdir
        
        # list of directories (with path, including subdirectories) to watch for changes to save as output files
        self.watchdir_recurs = watchdir_recurs
        
        #self.date_time
        
        ##################################
        # Submission status:
        
        # jobID
        self.jobID = None
        
    #
    
    def qsub_string(self):
        """Write this Job as a string"""
        
        s = "#!/bin/sh\n"
        s += "#PBS -S /bin/sh\n"
        s += "#PBS -N {0}\n".format(self.name)
        if self.exetime is not None:
            s += "#PBS -a {0}\n".format(self.exetime)
        if self.account is not None:
            s += "#PBS -A {0}\n".format(self.account)
        s += "#PBS -l walltime={0}\n".format(self.walltime)
        s += "#PBS -l nodes={0}:ppn={1}\n".format(self.nodes, self.ppn)
        if self.pmem is not None:
            s += "#PBS -l pmem={0}\n".format(self.pmem)
        s += "#PBS -l qos={0}\n".format(self.queue)
        s += "#PBS -q {0}\n".format(self.queue)
        if self.email != None and self.message != None:
            s += "#PBS -M {0}\n".format(self.email)
            s += "#PBS -m {0}\n".format(self.message)
        s += "#PBS -V\n"
        s += "#PBS -p {0}\n\n".format(self.priority)
        s += "#auto={0}\n\n".format(self.auto)
        if input != []:
            s += "#input={0}\n\n".format(" ".join(self.input))
        if output != []:
            s += "#output={0}\n\n".format(" ".join(self.output))
        if watchdir != []:
            s += "#watchdir={0}\n\n".format(" ".join(self.watchdir))
        if watchdir_recurs != []:
            s += "#watchdir_recurs={0}\n\n".format(" ".join(self.watchdir_recurs))
        s += "echo \"I ran on:\"\n"
        s += "cat $PBS_NODEFILE\n\n"
        s += "cd $PBS_O_WORKDIR\n"
        s += "python -c \"import pbs; pbs.start_job()\""
        s += "{0}\n".format(self.command)
        s += "python -c \"import pbs; pbs.end_job()\""
        
        return s
    
    def script(self, filename = "submit.sh"):
        """Write this Job as a bash script
        
        Keyword arguments:
        filename -- name of the script (default "submit.sh")
        
        """
        file = open(filename, 'w');
        file.write(self.qsub_string())
        file.close()
    
    def submit(self, add=True, dbpath=None):
        """Submit this Job using qsub
        
           add: Should this job be added to the JobDB database?
           dbpath: Specify a non-default JobDB database
           
           Returns a tuple: (returnCode, message)
             returnCode: 0 for success, 1 for error
             message: error message if error, jobID if success
        
        """
        
        self.jobID = misc.submit(qsubstr=self.qsub_string())
        if add:
            db = jobdb.JobDB(dbpath=dbpath)
            
            wlist = [file.WatchDir(name=wd, recursive=False, filelist=[]) for wd in self.watchdir] + \
                    [file.WatchDir(name=wd, recursive=True, filelist=[]) for wd in self.watchdir_recurs]
            
            status = jobdb.job_status_dict(jobid = self.jobID, \
                       jobname = self.name, \
                       rundir = os.getcwd(), \
                       jobstatus = "?", \
                       auto = self.auto, \
                       qsubstr = self.qsub_string(), \
                       walltime = misc.seconds(self.walltime), \
                       nodes = self.nodes, \
                       procs = self.nodes*self.ppn, \
                       input = file.FileList(self.input).serialize(), \
                       output = file.FileList(self.output).serialize(), \
                       watchdir = file.WatchDirList(wlist).serialize())
            db.add(status)
            db.close()
        return 0
    
    
    def read(self, qsubstr):
        """Set this Job object from string representing a PBS submit script.
        
           Will read many but not all valid PBS scripts.
           Will ignore any arguments not included in pbs.Job()'s attributes.
           Will add default optional arguments (-A, -a, -l pmem=(.*), -M, -m, -p, "auto=", "input=", "output=", "watchdir=", "watchdir_recurs=") if not found
           Will exit() if required arguments (-N, -l walltime=(.*), -l nodes=(.*):ppn=(.*), -q, cd $PBS_O_WORKDIR) not found
           Will always include -V
           
        """
        s = StringIO.StringIO(qsubstr)
        
        self.pmem = None
        self.email = None
        self.message = "a"
        self.priority = "0"
        self.auto = False
        self.input = []
        self.output = []
        self.watchdir = []
        self.watchdir_recurs = []
        self.account = None
        self.exetime = None
        
        optional = dict()
        optional["account"] = "Default: None"
        optional["pmem"] = "Default: None"
        optional["email"] = "Default: None"
        optional["message"] = "Default: a"
        optional["priority"] = "Default: 0"
        optional["auto"] = "Default: False"
        optional["input"] = "Default: []"
        optional["output"] = "Default: []"
        optional["watchdir"] = "Default: []"
        optional["watchdir_recurs"] = "Default: []"
        optional["exetime"] = "Default: None"
        
        required = dict()
        required["name"] = "Not Found"
        required["walltime"] = "Not Found"
        required["nodes"] = "Not Found"
        required["ppn"] = "Not Found"
        required["queue"] = "Not Found"
        required["cd $PBS_O_WORKDIR"] = "Not Found"
        required["command"] = "Not Found"
        
        while True:
            line = s.readline()
            #print line,
            
            if re.search("#PBS",line):
                
                m = re.search("-N\s+(.*)\s",line)
                if m:
                    self.name = m.group(1)
                    required["name"] = self.name
                
                m = re.search("-A\s+(.*)\s", line)
                if m:
                    self.account = m.group(1)
                    optional["account"] = self.account
                
                m = re.search("-a\s+(.*)\s", line)
                if m:
                    self.exetime = m.group(1)
                    optional["exetime"] = self.exetime
                
                m = re.search("\s-l\s", line)
                if m:
                    m = re.search("walltime=(.*)\s", line)
                    if m:
                        self.walltime = m.group(1)
                        required["walltime"] = self.walltime
                    
                    m = re.search("nodes=(.*):ppn=(.*)\s",line)
                    if m:
                        self.nodes = int(m.group(1))
                        self.ppn = int(m.group(2))
                        required["nodes"] = self.nodes
                        required["ppn"] = self.ppn
                    
                    m = re.search("pmem=(.*)\s",line)
                    if m:
                        self.pmem = m.group(1)
                        optional["pmem"] = self.pmem
                #
                
                m = re.search("-q\s+(.*)\s", line)
                if m:
                    self.queue = m.group(1)
                    required["queue"] = self.queue
                
                m = re.match("-M\s+(.*)\s", line)
                if m:
                    self.email = m.group(1)
                    optional["email"] = self.email
                
                m = re.match("-m\s+(.*)\s", line)
                if m:
                    self.message = m.group(1)
                    optional["message"] = self.message
                
                m = re.match("-p\s+(.*)\s", line)
                if m:
                    self.priority = m.group(1)
                    optional["priority"] = self.priority
            #
            
            m = re.search("auto=\s*(.*)\s", line)
            if m:
                if re.match("[fF](alse)*|0", m.group(1)):
                    self.auto = False
                    optional["auto"] = self.auto
                elif re.match("[tT](rue)*|1", m.group(1)):
                    self.auto = True
                    optional["auto"] = self.auto
                else:
                    print "Error in pbs.Job().read(). '#auto=' argument not understood:", line
                    sys.exit()
            
            m = re.search("input=\s*(.*)\s", line)
            if m:
                self.input = [os.path.abspath(x) for x in m.group(1).split()]
                optional["input"] = self.input
            
            m = re.search("output=\s*(.*)\s", line)
            if m:
                self.output = [os.path.abspath(x) for x in m.group(1).split()]
                optional["output"] = self.output
            
            m = re.search("watchdir=\s*(.*)\s", line)
            if m:
                self.watchdir = [os.path.abspath(x) for x in m.group(1).split()]
                optional["watchdir"] = self.watchdir
            
            m = re.search("watchdir_recurs=\s*(.*)\s", line)
            if m:
                self.watchdir_recurs = [os.path.abspath(x) for x in m.group(1).split()]
                optional["watchdir_recurs"] = self.watchdir_recurs
            
            m = re.search("cd\s+\$PBS_O_WORKDIR\s+", line)
            if m:
                required["cd $PBS_O_WORKDIR"] = "Found"
                self.command = ""
                for line in s: 
                    m = re.match("pstart|pend", line)
                    if not m:
                        self.command += line
                required["command"] = self.command
                break
        # end for
        
        # check for required arguments
        for k in required.keys():
            if required[k] == "Not Found":
                
                print "Error in pbs.Job.read(). Not all required arguments were found.\n"
                
                # print what we found:
                print "Optional arguments:"
                for k,v in optional.iteritems():
                    print k + ":", v
                print "\nRequired arguments:"
                for k,v in required.iteritems():
                    print k + ":", v
                
                sys.exit()
        # end if
    # end def







