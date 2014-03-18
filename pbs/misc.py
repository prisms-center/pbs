import subprocess, os, StringIO, re, datetime, time

class PBSError(Exception):
    def __init__(self, jobid, msg):
        self.jobid = jobid
        self.msg = msg
    
    def __str__(self):
        return self.jobid + ": " + self.msg


def getlogin():
    """Returns os.getlogin(), else os.environ["LOGNAME"], else "?" """
    try:
        return os.getlogin()
    except OSError:
        return os.environ["LOGNAME"]
    else:
        return "?"


def seconds( walltime):
    """Convert [[[DD:]HH:]MM:]SS to hours"""
    wtime = walltime.split(":")
    if( len(wtime)==1):
        return float(wtime[0])
    elif( len(wtime)==2):
        return float(wtime[0])*60.0 + float(wtime[1])
    elif( len(wtime)==3):
        return float(wtime[0])*3600.0 + float(wtime[1])*60.0 + float(wtime[2])
    elif( len(wtime)==4):
        return float(wtime[0])*24.0*3600.0 + float(wtime[0])*3600.0 + float(wtime[1])*60.0 + float(wtime[2])
    else:
        print "Error in walltime format:", walltime
        sys.exit()


def hours( walltime):
    """Convert [[[DD:]HH:]MM:]SS to hours"""
    wtime = walltime.split(":")
    if( len(wtime)==1):
        return float(wtime[0])/3600.0
    elif( len(wtime)==2):
        return float(wtime[0])/60.0 + float(wtime[1])/3600.0
    elif( len(wtime)==3):
        return float(wtime[0]) + float(wtime[1])/60.0 + float(wtime[2])/3600.0
    elif( len(wtime)==4):
        return float(wtime[0])*24.0 + float(wtime[0]) + float(wtime[1])/60.0 + float(wtime[2])/3600.0
    else:
        print "Error in walltime format:", walltime
        sys.exit()


def strftimedelta( seconds):
    """Convert seconds to D+:HH:MM:SS"""
    seconds = int(seconds)
    
    day_in_seconds = 24.0*3600.0
    hour_in_seconds = 3600.0
    minute_in_seconds = 60.0
    
    day = int(seconds/day_in_seconds)
    seconds -= day*day_in_seconds
    
    hour = int(seconds/hour_in_seconds)
    seconds -= hour*hour_in_seconds
    
    minute = int(seconds/minute_in_seconds)
    seconds -= minute*minute_in_seconds
    
    return str(day) + ":" + ("%02d" % hour) + ":" + ("%02d" % minute) + ":" + ("%02d" % seconds)


def exetime( deltatime):
    """Get the exetime string for the PBS '-a'option from a [[[DD:]MM:]HH:]SS string
    
       exetime string format: YYYYmmddHHMM.SS
    """
    return (datetime.datetime.now()+datetime.timedelta(hours=hours(deltatime))).strftime("%Y%m%d%H%M.%S")


def qstat(jobid=None, username=getlogin(), full=False):
    """Return the stdout of qstat minus the header lines.
       
       By default, 'username' is set to the current user.
       'full' is the '-f' option
       'id' is a string or list of strings of job ids
       
       Returns the text of qstat, minus the header lines
    """
    
    # set options
    opt = ["qstat"]
    if username != None:
        opt += ["-u", username]
    if full == True:
        opt += ["-f"]
    if jobid != None:
        if isinstance(jobid,str) or isinstance(jobid,unicode):
            jobid = [jobid]
        elif isinstance(jobid,list):
            pass
        else:
            print "Error in pbs.misc.qstat(). type(jobid):", type(jobid)
            sys.exit()
        opt += jobid
    
    # call 'qstat' using subprocess
    p = subprocess.Popen(opt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate()
    
    sout = StringIO.StringIO(stdout)
    
    # strip the header lines
    if full == False:
        for line in sout:
            if line[0] == "-":
                break
    
    # return the remaining text
    return sout.read()


def job_id(all=False,name=None):
    """If 'name' given, returns a list of all jobs with a particular name using qstat.
       Else, if all=True, returns a list of all job ids by current user. 
       Else, returns this job id from environment variable PBS_JOBID (split to get just the number).
       
       Else, returns None
       
    """
    if all or name != None:
        jobid = []
        stdout = qstat()
        sout = StringIO.StringIO(stdout)
        for line in sout:
            if name != None:
                if line.split()[3] == name:
                    jobid.append( (line.split()[0]).split(".")[0] )
            else:
                jobid.append( (line.split()[0]).split(".")[0] )
        return jobid
    else:
        if 'PBS_JOBID' in os.environ: 
            return os.environ['PBS_JOBID'].split(".")[0]
        else:
            return None
            #raise PBSError("?", "Could not determine jobid. 'PBS_JOBID' environment variable not found.\n" + str(os.environ))


def job_rundir( jobid):
    """Return the directory job "id" was run in using qstat.
       
       Returns a dict, with id as key and rundir and value.
    """
    rundir = dict()
    
    if isinstance(id, (list)):
        for i in jobid:
            stdout = qstat(jobid=i, full=True)
            match = re.search(",PWD=(.*),",stdout)
            rundir[i] = match.group(1)
    else:
        stdout = qstat(jobid=jobid, full=True)
        match = re.search(",PWD=(.*),",stdout)
        rundir[i] = match.group(1)
    return rundir

    
def job_status( jobid=None):
    """Return job status using qstat
    
       Returns a dict of dict, with jobid as key in outer dict. 
       Inner dict contains:
       "name", "nodes", "procs", "walltime", 
       "jobstatus": status ("Q","C","R", etc.)
       "qstatstr": result of qstat -f jobid, None if not found
       "elapsedtime": None if not started, else seconds as int
       "starttime": None if not started, else seconds since epoch as int
       "completiontime": None if not completed, else seconds since epoch as int
       
       *This should be edited to return job_status_dict()'s*
    """
    status = dict()
    
    stdout = qstat(jobid=jobid, full=True)
    sout = StringIO.StringIO(stdout)
    
    jobstatus = None
    
    for line in sout:
        
        m = re.search("Job Id:\s*(.*)\s",line)
        if m:
            if jobstatus != None:
                if jobstatus["jobstatus"] == "R":
                    jobstatus["elapsedtime"] = int(time.time()) - jobstatus["starttime"]
                status[jobstatus["jobid"]] = jobstatus
            jobstatus = dict()
            jobstatus["jobid"] = m.group(1).split(".")[0]
            jobstatus["qstatstr"] = line
            jobstatus["elapsedtime"] = None
            jobstatus["starttime"] = None
            jobstatus["completiontime"] = None
            continue
        
        jobstatus["qstatstr"] += line
        
        #results = line.split()
        #jobid = results[0].split(".")[0]
        #jobstatus = dict()
        #jobstatus["jobid"] = jobid
        
        #jobstatus["jobname"] = results[3]
        m = re.match("\s*Job_Name\s*=\s*(.*)\s",line)
        if m:
            jobstatus["jobname"] = m.group(1)
            continue
        
        #jobstatus["nodes"] = int(results[5])
        #jobstatus["procs"] = int(results[6])
        m = re.match("\s*Resource_List\.nodes\s*=\s*(.*):ppn=(.*)\s",line)
        if m:
            jobstatus["nodes"] = m.group(1)
            jobstatus["procs"] = int(m.group(1))*int(m.group(2))
            continue
        
        #jobstatus["walltime"] = int(seconds(results[8]))
        m = re.match("\s*Resource_List\.walltime\s*=\s*(.*)\s",line)
        if m:
            jobstatus["walltime"] = int(seconds(m.group(1)))
            continue
        
        #jobstatus["jobstatus"] = results[9]
        m = re.match("\s*job_state\s*=\s*(.*)\s",line)
        if m:
            jobstatus["jobstatus"] = m.group(1)
            continue
        
        #elapsedtime = line.split()[10]
        #if elapsedtime == "--":
        #    jobstatus["elapsedtime"] = None
        #else:
        #    jobstatus["elapsedtime"] = int(seconds(elapsedtime))
        #
        #qstatstr = qstat(jobid, full=True)
        #if not re.match("^qstat: Unknown Job Id Error.*",qstatstr):
        #    jobstatus["qstatstr"] = qstatstr
        #    m = re.search("Job_Name = (.*)\n",qstatstr)
        #    if m:
        #        jobstatus["jobname"] = m.group(1)
        
        #m = re.match("\s*resources_used.walltime\s*=\s*(.*)\s",line)
        #if m:
        #    print line
        #    jobstatus["elapsedtime"] = int(seconds(m.group(1)))
            
        m = re.match("\s*start_time\s*=\s*(.*)\s",line)
        if m:
            jobstatus["starttime"] = int( time.mktime(datetime.datetime.strptime(m.group(1),"%a %b %d %H:%M:%S %Y").timetuple()) )
            continue
            
        m = re.search("\s*comp_time\s*=\s*(.*)\s",line)
        if m:
            jobstatus["completiontime"] = int( time.mktime(datetime.datetime.strptime(m.group(1),"%a %b %d %H:%M:%S %Y").timetuple()) )
            continue
        
    if jobstatus != None:
        if jobstatus["jobstatus"] == "R":
            jobstatus["elapsedtime"] = int(time.time()) - jobstatus["starttime"]
        status[jobstatus["jobid"]] = jobstatus
    
    return status


def submit(qsubstr):
    """Submit a PBS job using qsub.
       
       qsubstr: The submit script string
    """
    
    m = re.search("-N\s+(.*)\s",qsubstr)
    if m:
        jobname = m.group(1)
    else:
        raise PBSError("Error in pbs.misc.submit(). Jobname (\"-N\s+(.*)\s\") not found in submit string.")
    
    p = subprocess.Popen("qsub", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate(input=qsubstr)
    print stdout[:-1]
    if re.search("error", stdout):
        raise PBSError("PBS Submission error.\n" + stdout + "\n" + stderr)
    else:
        jobid = stdout.split(".")[0]
        return jobid


def delete(jobid):
    """qdel a PBS job."""
    p = subprocess.Popen(["qdel", jobid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate()
    return p.returncode


def hold(jobid):
    """qhold a PBS job."""
    p = subprocess.Popen(["qhold", jobid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate()
    return p.returncode


def release(jobid):
    """qrls a PBS job."""
    p = subprocess.Popen(["qrls", jobid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate()
    return p.returncode


def alter(jobid, arg):
    """qalter a PBS job.
    
        'arg' is a pbs command option string. For instance, "-a 201403152300.19"
    """
    p = subprocess.Popen(["qalter"] + arg.split() + [jobid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate()
    return p.returncode



