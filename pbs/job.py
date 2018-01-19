""" Class for individual Job objects """
### External ###
# import subprocess
import re
import os
import sys
import StringIO

### Local ###
import jobdb
import misc

class Job(object):  #pylint: disable=too-many-instance-attributes
    """A qsub Job object.

    Initialize either with all the parameters, or with 'qsubstr' a PBS submit script as a string.
    If 'qsubstr' is given, all other arguments are ignored and set using Job.read().


    Contains variables (with example values):
        name        "jobname"
        account     "prismsproject_fluxoe"
        nodes       2
        ppn         16
        walltime    "10:00:00"
        pmem        "3800mb"
        qos         "flux"
        queue       "fluxoe"
        exetime     "1100"
        message     "abe"
        email       "jdoe@umich.edu"
        priority    "-200"
        command     "echo \"hello\" > test.txt"
        auto        True
        software    "torque"

        Only set to auto=True if the 'command' uses this pbs module to set itself as
            completed when it is completed.
           Otherwise, you may submit it extra times leading to wasted resources and
            overwritten data.


    """


    def __init__(self, name="STDIN", account=None, nodes=None, ppn=None, walltime=None, #pylint: disable=too-many-arguments, too-many-locals
                 pmem=None, qos=None, queue=None, exetime=None, message="a", email=None,
                 priority="0", command=None, auto=False, substr=None, software=None):

        if substr != None:
            self.read(substr)
            return

        # Determines the software and loads the appropriate package
        if software is None:
            software = misc.getsoftware()
        self.software = software

        global misc_pbs
        if self.software.strip() == "torque":
		misc_pbs = __import__("pbs.misc_torque", globals(), locals(), [], -1).misc_torque
	elif self.software.strip() == "slurm":
		misc_pbs = __import__("pbs.misc_slurm", globals(), locals(), [], -1).misc_slurm
	else:
		misc_pbs = __import__("pbs.misc_torque", globals(), locals(), [], -1).misc_torque

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

        # qos string
        self.qos = qos

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

        #self.date_time

        ##################################
        # Submission status:

        # jobID
        self.jobID = None   #pylint: disable=invalid-name

    #

    def sub_string(self):   #pylint: disable=too-many-branches
        """ Write Job as a string suitable for self.software """
        if self.software.lower() == "slurm":
            ###Write this Job as a string suitable for slurm
            ### NOT USED:
            ###    exetime
            ###    priority
            ###    auto
            jobstr = "#!/bin/sh\n"
            jobstr += "#SBATCH -J {0}\n".format(self.name)
            if self.account is not None:
                jobstr += "#SBATCH -A {0}\n".format(self.account)
            jobstr += "#SBATCH -t {0}\n".format(self.walltime)
            jobstr += "#SBATCH -n {0}\n".format(self.nodes*self.ppn)
            if self.pmem is not None:
                jobstr += "#SBATCH --mem-per-cpu={0}\n".format(self.pmem)
            if self.qos is not None:
                jobstr += "#SBATCH --qos={0}\n".format(self.qos)
            if self.email != None and self.message != None:
                jobstr += "#SBATCH --mail-user={0}\n".format(self.email)
                if 'b' in self.message:
                    jobstr += "#SBATCH --mail-type=BEGIN\n"
                if 'e' in self.message:
                    jobstr += "#SBATCH --mail-type=END\n"
                if 'a' in self.message:
                    jobstr += "#SBATCH --mail-type=FAIL\n"
            # SLURM does assignment to no. of nodes automatically
            # jobstr += "#SBATCH -N {0}\n".format(self.nodes)
            if self.queue is not None:
                jobstr += "#SBATCH -p {0}\n".format(self.queue)
            jobstr += "{0}\n".format(self.command)

            return jobstr

        else:
            ###Write this Job as a string suitable for torque###

            jobstr = "#!/bin/sh\n"
            jobstr += "#PBS -S /bin/sh\n"
            jobstr += "#PBS -N {0}\n".format(self.name)
            if self.exetime is not None:
                jobstr += "#PBS -a {0}\n".format(self.exetime)
            if self.account is not None:
                jobstr += "#PBS -A {0}\n".format(self.account)
            jobstr += "#PBS -l walltime={0}\n".format(self.walltime)
            jobstr += "#PBS -l nodes={0}:ppn={1}\n".format(self.nodes, self.ppn)
            if self.pmem is not None:
                jobstr += "#PBS -l pmem={0}\n".format(self.pmem)
            if self.qos is not None:
                jobstr += "#PBS -l qos={0}\n".format(self.qos)
            if self.queue is not None:
                jobstr += "#PBS -q {0}\n".format(self.queue)
            if self.email != None and self.message != None:
                jobstr += "#PBS -M {0}\n".format(self.email)
                jobstr += "#PBS -m {0}\n".format(self.message)
            jobstr += "#PBS -V\n"
            jobstr += "#PBS -p {0}\n\n".format(self.priority)
            jobstr += "#auto={0}\n\n".format(self.auto)
            jobstr += "echo \"I ran on:\"\n"
            jobstr += "cat $PBS_NODEFILE\n\n"
            jobstr += "cd $PBS_O_WORKDIR\n"
            jobstr += "{0}\n".format(self.command)

            return jobstr



    def script(self, filename="submit.sh"):
        """Write this Job as a bash script

        Keyword arguments:
        filename -- name of the script (default "submit.sh")

        """
        with open(filename, "w") as myfile:
            myfile.write(self.sub_string())

    def submit(self, add=True, dbpath=None, configpath=None):
        """Submit this Job using qsub

           add: Should this job be added to the JobDB database?
           dbpath: Specify a non-default JobDB database

           Raises PBSError if error submitting the job.

        """
        try:
            self.jobID = misc_pbs.submit(substr=self.sub_string())
        except misc.PBSError as e:  #pylint: disable=invalid-name
            raise e

        if add:
            db = jobdb.JobDB(dbpath=dbpath, configpath=configpath) #pylint: disable=invalid-name
            status = jobdb.job_status_dict(jobid=self.jobID, jobname=self.name,
                                           rundir=os.getcwd(), jobstatus="?",
                                           auto=self.auto, qsubstr=self.sub_string(),
                                           walltime=misc.seconds(self.walltime),
                                           nodes=self.nodes, procs=self.nodes*self.ppn)
            db.add(status)
            db.close()


    def read(self, qsubstr):    #pylint: disable=too-many-branches, too-many-statements
        """Set this Job object from string representing a PBS submit script.

           Will read many but not all valid PBS scripts.
           Will ignore any arguments not included in pbs.Job()'s attributes.
           Will add default optional arguments (-A, -a, -l pmem=(.*), -l qos=(.*),
                -M, -m, -p, "Auto:") if not found
           Will exit() if required arguments (-N, -l walltime=(.*), -l nodes=(.*):ppn=(.*),
                -q, cd $PBS_O_WORKDIR) not found
           Will always include -V

        """
        s = StringIO.StringIO(qsubstr)  #pylint: disable=invalid-name

        self.pmem = None
        self.email = None
        self.message = "a"
        self.priority = "0"
        self.auto = False
        self.account = None
        self.exetime = None
        self.qos = None

        optional = dict()
        optional["account"] = "Default: None"
        optional["pmem"] = "Default: None"
        optional["email"] = "Default: None"
        optional["message"] = "Default: a"
        optional["priority"] = "Default: 0"
        optional["auto"] = "Default: False"
        optional["exetime"] = "Default: None"
        optional["qos"] = "Default: None"

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

            if re.search("#PBS", line):

                m = re.search(r"-N\s+(.*)\s", line) #pylint: disable=invalid-name
                if m:
                    self.name = m.group(1)
                    required["name"] = self.name

                m = re.search(r"-A\s+(.*)\s", line)  #pylint: disable=invalid-name
                if m:
                    self.account = m.group(1)
                    optional["account"] = self.account

                m = re.search(r"-a\s+(.*)\s", line)  #pylint: disable=invalid-name
                if m:
                    self.exetime = m.group(1)
                    optional["exetime"] = self.exetime

                m = re.search(r"\s-l\s", line)   #pylint: disable=invalid-name
                if m:
                    m = re.search(r"walltime=([0-9:]+)", line)   #pylint: disable=invalid-name
                    if m:
                        self.walltime = m.group(1)
                        required["walltime"] = self.walltime

                    m = re.search(r"nodes=([0-9]+):ppn=([0-9]+)", line)   #pylint: disable=invalid-name
                    if m:
                        self.nodes = int(m.group(1))
                        self.ppn = int(m.group(2))
                        required["nodes"] = self.nodes
                        required["ppn"] = self.ppn

                    m = re.search(r"pmem=([^,\s]+)", line)    #pylint: disable=invalid-name
                    if m:
                        self.pmem = m.group(1)
                        optional["pmem"] = self.pmem

                    m = re.search(r"qos=([^,\s]+)", line) #pylint: disable=invalid-name
                    if m:
                        self.qos = m.group(1)
                        optional["qos"] = self.qos
                #

                m = re.search(r"-q\s+(.*)\s", line)  #pylint: disable=invalid-name
                if m:
                    self.queue = m.group(1)
                    required["queue"] = self.queue

                m = re.match(r"-M\s+(.*)\s", line) #pylint: disable=invalid-name
                if m:
                    self.email = m.group(1)
                    optional["email"] = self.email

                m = re.match(r"-m\s+(.*)\s", line) #pylint: disable=invalid-name
                if m:
                    self.message = m.group(1)
                    optional["message"] = self.message

                m = re.match(r"-p\s+(.*)\s", line)   #pylint: disable=invalid-name
                if m:
                    self.priority = m.group(1)
                    optional["priority"] = self.priority
            #

            m = re.search(r"auto=\s*(.*)\s", line)   #pylint: disable=invalid-name
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

            m = re.search(r"cd\s+\$PBS_O_WORKDIR\s+", line)  #pylint: disable=invalid-name
            if m:
                required["cd $PBS_O_WORKDIR"] = "Found"
                self.command = s.read()
                required["command"] = self.command
                break
        # end for

        # check for required arguments
        for k in required.keys():
            if required[k] == "Not Found":

                print "Error in pbs.Job.read(). Not all required arguments were found.\n"

                # print what we found:
                print "Optional arguments:"
                for k, v in optional.iteritems():    #pylint: disable=invalid-name
                    print k + ":", v
                print "\nRequired arguments:"
                for k, v in required.iteritems():    #pylint: disable=invalid-name
                    if k == "command":
                        print k + ":"
                        print "--- Begin command ---"
                        print v
                        print "--- End command ---"
                    else:
                        print k + ":", v

                sys.exit()
        # end if
    # end def







