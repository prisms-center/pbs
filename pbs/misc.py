import subprocess, os, StringIO, re, datetime, time

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
    
    stdout = qstat(jobid=jobid)
    sout = StringIO.StringIO(stdout)
    for line in sout:
        jobstatus = dict()
        
        results = line.split()
        jobid = results[0].split(".")[0]
        jobstatus["jobid"] = jobid
        jobstatus["jobname"] = results[3]
        jobstatus["nodes"] = int(results[5])
        jobstatus["procs"] = int(results[6])
        jobstatus["walltime"] = int(seconds(results[8]))
        jobstatus["jobstatus"] = results[9]
        jobstatus["qstatstr"] = None
        jobstatus["elapsedtime"] = None
        jobstatus["starttime"] = None
        jobstatus["completiontime"] = None
        elapsedtime = line.split()[10]
        if elapsedtime == "--":
            jobstatus["elapsedtime"] = None
        else:
            jobstatus["elapsedtime"] = int(seconds(elapsedtime))
        
        qstatstr = qstat(jobid, full=True)
        if not re.match("^qstat: Unknown Job Id Error.*",qstatstr):
            jobstatus["qstatstr"] = qstatstr
            m = re.search("Job_Name = (.*)\n",qstatstr)
            if m:
                jobstatus["jobname"] = m.group(1)
            
            m = re.search("start_time = (.*)\n",qstatstr)
            if m:
                jobstatus["starttime"] = int( time.mktime(datetime.datetime.strptime(m.group(1),"%a %b %d %H:%M:%S %Y").timetuple()) )
            
            m = re.search("comp_time = (.*)\n",qstatstr)
            if m:
                jobstatus["completiontime"] = int( time.mktime(datetime.datetime.strptime(m.group(1),"%a %b %d %H:%M:%S %Y").timetuple()) )
        
        #print "JobID:", jobid
        #for k,val in jobstatus.iteritems():
        #    print "  ", k, ":", val

        
        status[jobid] = jobstatus
    
    return status


def submit(qsubstr):
    """Submit a PBS job using qsub.
       
       qsubstr: The submit script string
    """
    
    m = re.search("-N\s+(.*)\s",qsubstr)
    if m:
        jobname = m.group(1)
    else:
        print "Error in pbs.misc.submit(). Jobname (\"-N\s+(.*)\s\") not found in submit string."
        sys.exit()
     
    
    p = subprocess.Popen("qsub", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = p.communicate(input=qsubstr)
    print stdout[:-1]
    if re.search("error", stdout):
        return (1, stdout)
    else:
        jobID = stdout.split(".")[0]
        return (0, jobID)


def delete(jobid):
    """qdel a PBS job."""
    p = subprocess.Popen(["qdel", jobid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.returncode



