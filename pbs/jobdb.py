import sqlite3, os, sys, socket, time, re, subprocess
import misc

# columns in database (see job_status_dict()):
# username, hostname, jobid, jobname, rundir, jobstatus, auto, taskstatus, continuation_jobid, qsubstr, 
# qstatstr, nodes, proc, walltime, starttime, completiontime, elapsedtime

# allowed values (not checked at this time):
# taskstatus = ["Incomplete","Continued","Check","Complete","Error:.*","Aborted"]
# jobstatus = ["C","Q","R","E","W","H","M"]
# auto=[1,0]

def job_status_dict(   username = misc.getlogin(), \
                       hostname = socket.gethostname(), \
                       jobid = "-", \
                       jobname = "-", \
                       rundir = "-", \
                       jobstatus = "-", \
                       auto = 0, \
                       taskstatus = "Incomplete", \
                       continuation_jobid = "-", \
                       qsubstr = "-", \
                       qstatstr = "-", \
                       nodes = None, \
                       procs = None, \
                       walltime = None, \
                       elapsedtime = None, \
                       starttime = None, \
                       completiontime = None):
    """Return a dict() with job_status fields.
    
       This is used to add records to the JobDB database through JobDB().add().
    """
    
    creationtime = int(time.time())
    modifytime = creationtime
    
    status = dict()
    status["username"] = username
    status["hostname"] = hostname
    status["jobid"] = jobid
    status["jobname"] = jobname
    status["rundir"] = rundir
    status["jobstatus"] = jobstatus
    status["auto"] = int(bool(auto))
    status["taskstatus"] = taskstatus
    status["continuation_jobid"] = continuation_jobid
    status["qsubstr"] = qsubstr
    status["qstatstr"] = qstatstr
    
    # integer:
    status["nodes"] = nodes
    status["procs"] = procs
    
    # integer s:
    status["walltime"] = walltime
    status["elapsedtime"] = elapsedtime
        
    # integer s since the epoch:
    status["creationtime"] = creationtime
    status["starttime"] = starttime
    status["completiontime"] = completiontime
    status["modifytime"] = modifytime
    
    return status


#def selector_rules():
#    rules = dict()
#    rules["username"] = lambda r: r["username"]
#    rules["hostname"] = lambda r: r["hostname"]
#    rules["jobid"] = lambda r: r["jobid"]
#    rules["rundir"] = lambda r: r["rundir"]
#    rules["jobstatus"] = lambda r: r["jobstatus"]
#    rules["auto"] = lambda r: r["auto"]
#    rules["taskstatus"] = lambda r: r["taskstatus"]
#    rules["continuation_jobid"] = lambda r: r["continuation_jobid"]
#    rules["qsubstr"] = lambda r: r["continuation_jobid"]
#    ...
#    return rules


def job_status_type_dict():
    """This specifies the SQL type for each field. 
       It is used to create the JobDB SQL table.
    """ 
    status = job_status_dict()
    for k in status.keys():
        status[k] = "text"
    status["auto"] = "integer"
    
    status["nodes"] = "integer"
    status["procs"] = "integer"
    
    status["walltime"] = "integer"
    status["elapsedtime"] = "integer"
    
    status["creationtime"] = "integer"
    status["starttime"] = "integer"
    status["completiontime"] = "integer"
    status["modifytime"] = "integer"
    
    return status


def sql_create_str():
    """Returns a string for SQL CREATE TABLE"""
    status_type = job_status_type_dict()
    s = "("
    for k in status_type.keys():
        s += k + " " + status_type[k] + ", "
    return s[:-2] + ")"


def sql_insert_str(job_status):
    """ Accepts job_status dict, Returns strings and tuple used for SQL INSERT INTO."""
    job_status["auto"] = int(bool(job_status["auto"]))
    colstr = "("
    questionstr = "("
    val = []
    for k in job_status.keys():
        colstr = colstr + k + ", "
        questionstr = questionstr + "?, "
        val.append(job_status[k])
    colstr = colstr[:-2] + ")"
    questionstr = questionstr[:-2] + ")"
    return colstr, questionstr, tuple(val)   


def sql_iter(curs, arraysize=1000):
    """ Iterate over the results of a SELECT statement """
    while True:
        records = curs.fetchmany(arraysize)
        if not records:
            break
        else:
            for r in records:
                yield r


class JobDB(object):
    """A PBS Job Database object"""

    def __init__(self, dbpath = None):
        """Construct a PBS Job Database object.
        
           Usually this is called without arguments (pbs.JobDB()) to open or create a database in the default location.
           
           If dbpath is not given, the default location is determined as follows:
             If the PBS_JOB_DB environment variable exists, set dbpath to "$PBS_JOB_DB/jobs.db" file.
             Else, set dbpath to "$HOME/.pbs/jobs.db", where $HOME is the user's home directory
           Else:
             dbpath: path to a JobDB database file.
           
           
        """
        
        self.conn = None
        self.curs = None
        
        self.username = misc.getlogin()
        self.hostname = socket.gethostname()
        self.connect(dbpath)
        
        # list of dict() from misc.job_status for jobs not tracked in database:
        # refreshed upon update()
        self.untracked = []
    
    
    def connect(self, dbpath=None):
        """Open a connection to the jobs database.
        
           dbpath: path to a JobDB database file.
           
           If dbpath is not given:
           If PBS_JOB_DB environment variable exists, set dbpath to "$PBS_JOB_DB/jobs.db" file.
           Else, set dbpath to "$HOME/.pbs/jobs.db", where $HOME is the user's home directory
           
        """
        
        if dbpath == None: 
            if "PBS_JOB_DB" in os.environ:
                dbpath = os.environ("PBS_JOB_DB")
                if not os.path.isdir(dbpath):
                    print "Error in pbs.jobdb.JobDB.connect()."
                    print "  PBS_JOB_DB:", dbpath
                    print "  Does not exist"
                    sys.exit()
            else:
                dbpath = os.path.join( os.path.expanduser("~"), ".pbs")
                if not os.path.isdir(dbpath):
                    print "Creating directory:", dbpath
                    os.mkdir(dbpath)
            dbpath = os.path.join(dbpath,"jobs.db")
        else:
            if not os.path.isfile(dbpath):
                print "Error in pbs.jobdb.JobDB.connect(). argument dbpath =", dbpath, "is not a file."
                sys.exit()
        
        if not os.path.isfile(dbpath):
            print "Creating Database:", dbpath
            self.conn = sqlite3.connect(dbpath)
            self.conn.row_factory = sqlite3.Row
            self.curs = self.conn.cursor()
            self.curs.execute("CREATE TABLE jobs " + sql_create_str())
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(dbpath)
            self.conn.row_factory = sqlite3.Row
            self.curs = self.conn.cursor()
        
    
    def close(self):
        """Close the connection to the jobs database."""
        
        self.conn.close()
    
    
    def add(self, job_status ):
        """Add a record to the jobs database.
        
           Accepts 'job_status', a dictionary of data comprising the record. Create
           'job_status' using pbs.jobdb.job_status_dict().
        
        """
        (colstr, questionstr, valtuple) = sql_insert_str(job_status)
        insertstr = "INSERT INTO jobs {0} VALUES {1}".format( colstr, questionstr)
        self.curs.execute( insertstr , valtuple)
        self.conn.commit()
    
    
    def update(self):
        """Update records using qstat.
        
           Any jobs found using qstat that are not in the jobs database are saved in 'self.untracked'.
        """
        
        # update jobstatus

        # select jobs that are not yet marked complete
        self.curs.execute("SELECT jobid FROM jobs WHERE jobstatus<>'C'")
        
        # newstatus will contain the updated info
        newstatus = dict()
        
        # any jobs that we don't find with qstat should be marked as 'C'
        for f in sql_iter(self.curs):
            newstatus[f["jobid"]] = "C"
        
        # get job_status dict for all jobs found with qstat
        active_status = misc.job_status()
        
        # reset untracked
        self.untracked = []
        
        # collect job status
        for k in active_status.keys():
            if k in newstatus:
                newstatus[k] = active_status[k]
            else:
                self.curs.execute("SELECT jobid FROM jobs WHERE jobid=?",(k,))
                if self.curs.fetchone() is None:
                    self.untracked.append(active_status[k])
        
        # update database with latest job status
        for key, jobstatus in newstatus.iteritems():
            if jobstatus == "C":
                self.curs.execute("UPDATE jobs SET jobstatus=?, elapsedtime=?, modifytime=? WHERE jobid=?", \
                  ("C", None, int(time.time()), key))
            elif jobstatus["qstatstr"] == None:
                self.curs.execute("UPDATE jobs SET jobstatus=?, elapsedtime=?, modifytime=? WHERE jobid=?", \
                  (jobstatus["jobstatus"], jobstatus["elapsedtime"], int(time.time()), key))
            else:
                self.curs.execute("UPDATE jobs SET jobstatus=?, elapsedtime=?, starttime=?, completiontime=?, qstatstr=?, modifytime=? WHERE jobid=?", \
                  (jobstatus["jobstatus"], jobstatus["elapsedtime"], jobstatus["starttime"], jobstatus["completiontime"], "qstatstr", int(time.time()), key))
        self.conn.commit()
        
        # update taskstatus for non-auto jobs
        self.curs.execute("UPDATE jobs SET taskstatus='Check', modifytime=? WHERE jobstatus='C' AND taskstatus='Incomplete' AND auto=0", (int(time.time()),))
        self.conn.commit()
    
    
    def select_job(self, jobid):
        """Return record (sqlite3.Row object) for one job with given jobid."""
        if not isinstance(jobid,str) and not isinstance(jobid,unicode):
            print "Error in pbs.JobDB.select_job(). type(id):", type(jobid), "expected str."
            sys.exit()
        
        self.curs.execute("SELECT * FROM jobs WHERE jobid=?", (jobid,))
        r = self.curs.fetchall()
        if len(r) == 0:
            print "Error in pbs.JobDB.select_job(). jobid:", jobid, " not found."
            sys.exit()
        elif len(r) > 1:
            print "Error in pbs.JobDB.select_job().", len(r), " records with jobid:", jobid, " found."
            sys.exit()
        return r[0]
    
    
    def select_series(self, jobid):
        """Return records (sqlite3.Row objects) for a series of auto jobs"""
        r = self.select_job(jobid)
        series = [r]
        parent = self.select_parent(jobid)
        while parent != None:
            series.insert(0,parent)
            parent = self.select_parent(parent["jobid"])
        child = self.select_child(jobid)
        while child != None:
            series.append(child)
            child = self.select_child(child["jobid"])
        return series
    
    
    #def select_id(self, logicstr=[], series=False): 
    #    """Select jobs using cllogic postfix notation logic string."""
    #    if True:
    #        print "pbs.jobdb.JobDB.select is not complete"
    #        sys.exit()
    #    #job = []
    #    #selector = cllogic.Selector(selection_rules())
    #    #self.curs.execute("SELECT * FROM jobs")
    #    #for r in sql_iter(self.curs):
    #    #    if selector(r,logicstr):
    #    #        job.append(r["jobid"])
    #    #return job
    
    
    def select_all_id(self, series=False):
        """Return a list with all jobids."""
        job = []
        self.curs.execute("SELECT * FROM jobs")
        for r in sql_iter(self.curs):
            if series == False or r["continuation_jobid"] == "-":
                job.append(r["jobid"])
            job.append(r["jobid"])
        return job
    
    
    def select_all_active_id(self, series=False):
        """Return a list with all active jobids.
        
           "Active" jobs are those with taskstatus='Incomplete' or 'Check'
        """
        active_job = []
        self.curs.execute("SELECT * FROM jobs WHERE taskstatus='Incomplete' OR taskstatus='Check'")
        for r in sql_iter(self.curs):
            active_job.append( r["jobid"] )
        return active_job
    
    
    def select_series_id(self, jobid):
        """Return a list with all jobids for a series of auto jobs."""
        job = [jobid]
        r = self.select_job(jobid)
        parent = self.select_parent(jobid)
        while parent != None:
            job.insert(0,parent["jobid"])
            parent = self.select_parent(parent["jobid"])
        child = self.select_child(jobid)
        while child != None:
            job.append(child["jobid"])
            child = self.select_child(child["jobid"])
        return job
    
    
    def select_all_series_id(self):
        """Return a list of lists of jobids (one list for each series)."""
        all_series = []
        self.curs.execute("SELECT * FROM jobs")
        for r in sql_iter(self.curs):
            if r["continuation_jobid"] == "-":
                all_series.append( select_series_id(r["jobid"]) )
        return all_series
    
    
    def select_active_series_id(self):
        """Return a list of lists of jobids (one list for each active series).
           
           "Active" series of auto jobs are those with one job with taskstatus='Incomplete' or 'Check'
        """
        active_series = []
        self.curs.execute("SELECT * FROM jobs WHERE taskstatus='Incomplete' OR taskstatus='Check'")
        for r in sql_iter(self.curs):
            if r["continuation_jobid"] == "-":
                active_series.append( select_series_id(r["jobid"]) )
        return active_series
    
    
    def select_parent(self, jobid):
        """Return record for the parent of a job
            
           The parent is the job with continuation_jobid = given jobid
        """
        if not isinstance(jobid,str) and not isinstance(jobid,unicode):
            print "Error in pbs.JobDB.select_parent(). type(id):", type(jobid), "expected str."
            sys.exit()
        
        self.curs.execute("SELECT * FROM jobs WHERE continuation_jobid=?",(jobid,))
        r = self.curs.fetchall()
        if len(r) == 0:
            return None
        elif len(r) > 1:
            print "Error in pbs.JobDB.select_parent().", len(r), " records with continuation_jobid:", jobid, " found."
            sys.exit()
        return r[0]
    
    
    def select_child(self, jobid):
        """Return record for the child of a job
        
           The child is the job whose jobid = continuation_jobid of job with given jobid
        """
        r = self.select_job(jobid)
        
        if r["continuation_jobid"] == "-":
            return None
        
        self.curs.execute("SELECT * FROM jobs WHERE jobid=?",(r["continuation_jobid"],))
        r = self.curs.fetchall()
        if len(r) == 0:
            print "Error in pbs.JobDB.select_child(). jobid:", jobid, " child:", r["continuation_jobid"], "not found."
            sys.exit()
        elif len(r) > 1:
            print "Error in pbs.JobDB.select_child().", len(r), " records with child jobid:", r["continuation_jobid"], " found."
            sys.exit()
        return r[0]
    
    
    #def note_job(self, jobid, note):
    #    """Add notes to a job"""
    #    # add notes here
    
    
    def continue_job(self, jobid):
        """Resubmit one job with given jobid.
        
           Job must have jobstatus="C" and taskstatus="Incomplete" and auto=1, or else sys.exit() called.
        """
        
        r = self.select_job(jobid)
        
        if r["jobstatus"] != "C":
            print "Error in pbs.JobDB.continue_job(). jobstatus =", r["jobstatus"]
            sys.exit()
        
        if r["taskstatus"] != "Incomplete":
            print "Error in pbs.JobDB.continue_job(). taskstatus =", r["taskstatus"]
            sys.exit()
        
        if r["auto"] != 1:
            print "Error in pbs.JobDB.continue_job(). auto =", bool(r["auto"])
            sys.exit()
        
        wd = os.getcwd()
        os.chdir( r["rundir"])
        
        result = misc.submit(qsubstr=r["qsubstr"])
        if result[0] == 0:
            self.curs.execute("UPDATE jobs SET taskstatus='Continued', modifytime=?, continuation_jobid=? WHERE jobid=?",(int(time.time()),result[1],jobid))
            status = job_status_dict(jobid = result[1], jobname = r["jobname"], rundir = os.getcwd(), \
                       jobstatus = "?", auto = r["auto"], qsubstr = r["qsubstr"], nodes = r["nodes"], \
                       procs = r["procs"], walltime = r["walltime"])
            self.add(status)
        os.chdir(wd)
        return result
    
    def continue_all(self):
        """Resubmit all auto jobs with jobstatus='C' and taskstatus='Incomplete'"""
        self.curs.execute("SELECT jobid FROM jobs WHERE auto=1 AND taskstatus='Incomplete' AND jobstatus='C'")
        for r in sql_iter(self.curs):
            self.continue_job(r["jobid"])
    
    
    def abort_job(self, jobid):
        """qdel job and mark job taskstatus as Aborted"""
        
        # verify jobid is valid
        self.select_job(jobid)
        
        misc.delete(jobid)
        self.curs.execute("UPDATE jobs SET taskstatus='Aborted', modifytime=? WHERE jobid=?",(int(time.time()),jobid))
        self.conn.commit()
    
    
    def error_job(self, jobid, message):
        """Mark job taskstatus as 'Error: message'"""
        message = "Error: " + message
        r = self.select_job(jobid)
        self.curs.execute("UPDATE jobs SET taskstatus=?, modifytime=? WHERE jobid=?",(message,int(time.time()),jobid))
        self.conn.commit()
    
    
    def complete_job(self, jobid):
        """Mark job taskstatus as 'Complete'"""
        
        # verify jobid is valid
        self.select_job(jobid)
        
        self.curs.execute("UPDATE jobs SET taskstatus='Complete', modifytime=? WHERE jobid=?",(int(time.time()),jobid))
        self.conn.commit()
    
    
    def print_header(self):
        """Print header rows for record summary"""
        print "{0:<12} {1:<24} {2:^5} {3:^5} {4:>12} {5:^1} {6:>12} {7:<24} {8:^1} {9:<12}".format("JobID","JobName","Nodes","Procs","Walltime","S","Runtime","Task","A","ContJobID") 
        print "{0:-^12} {1:-^24} {2:-^5} {3:-^5} {4:->12} {5:-^1} {6:->12} {7:-<24} {8:-^1} {9:-^12}".format("-","-","-","-","-","-","-","-","-","-") 
        
    
    def print_record(self, r):
        """Print record summary
        
            r: dict-like object containing: "jobid", "jobname", "nodes", "procs", 
                                            "walltime", "jobstatus", "elapsedtime", 
                                            "taskstatus", "auto", and "continuation_jobid"
        """
        
        d = dict(r)
        
        for k in ["walltime", "elapsedtime"]:
            if d[k] == None:
                d[k] = "-"
            elif isinstance(d[k], int):
                d[k] = misc.strftimedelta(d[k])
        
        print "{0:<12} {1:<24} {2:^5} {3:^5} {4:>12} {5:^1} {6:>12} {7:<24} {8:^1} {9:<12}".format(d["jobid"], d["jobname"], d["nodes"], d["procs"], d["walltime"], d["jobstatus"], d["elapsedtime"], d["taskstatus"], d["auto"], d["continuation_jobid"])
    
    
    def print_full_record(self, r):
        """Print record as list of key-val pairs.
        
            r: a dict-like object
        """
        print "#Record:"
        for key in r.keys():
            if isinstance(r[key],(str,unicode)):
                s = "\"" + r[key] + "\""
                if re.search("\n",s):
                    s = "\"\"" + s + "\"\""
                print key, "=", s
            else:
                print key, "=", r[key]
        print ""
    
    
    def print_job(self, jobid, full=False, series=False):
        """Print job with given jobid
        
           If full: print key-val pairs for all fields
           If series: also print other jobs in the series
        """
        
        if series:
            series = self.select_series(jobid)
            if full:
                for r in series:
                    self.print_full_record(r)
            else:
                for r in series:
                    self.print_record(r)
            print ""
        else:
            r = self.select_job(jobid)
            if full:
                self.print_full_record(r)
            else:
                self.print_record(r)
    
    
    def print_selected(self, curs=None, full=False, series=False):
        """Fetch and print jobs selected with SQL SELECT statement using cursor 'curs'. 
           
           
           Arguments:
             curs: Fetch selected jobs from sqlite3 cursor 'curs'. If no 'curs' given, use self.curs.
             full: If True, print as key:val pair list, If (default) False, print single row summary in 'qstat' style.
             series: If True, print records as groups of auto submitting job series. If (default) False, print in order found.
        """
        if curs == None:
            curs = self.curs
        if full:
            for r in sql_iter(self.curs):
                if series:
                    if r["continuation_jobid"] == "-":
                        self.print_job(r["jobid"],full=full,series=series)
                else:
                    self.print_full_record(r)
        else:
            for r in sql_iter(self.curs):
                if series:
                    if r["continuation_jobid"] == "-":
                        self.print_job(r["jobid"],full=full,series=series)
                else:
                    self.print_record(r)
    
    
    def print_untracked(self, full=False):
        """Print untracked jobs.
        
           Untracked jobs are stored in self.untracked after calling JobDB.update().
            
            Arguments:
             full: If True, print as key:val pair list, If (default) False, print single row summary in 'qstat' style.
        """
        if not full:
            self.print_header()
        sort = sorted(self.untracked, key=lambda rec: rec["jobid"])
        for r in sort:
            tmp = dict(r)
            tmp["continuation_jobid"] = "-"
            tmp["auto"] = 0
            tmp["taskstatus"] = "Untracked"
            if full:
                self.print_full_record(tmp)
            else:
                self.print_record(tmp)
    
    
    def print_all(self, full=False, series=False):
        """Print all jobs
        
           Arguments:
             full: If True, print as key:val pair list, If (default) False, print single row summary in 'qstat' style.
             series: If True, print records as groups of auto submitting job series. If (default) False, print in order found.
        """
        print "\n\nTracked:"
        self.curs.execute("SELECT * FROM jobs")
        if not full:
            self.print_header()
        self.print_selected(full=full, series=series)
        print "\n\nUntracked:"
        self.print_untracked(full=full)
    
    
    def print_active(self, full=False, series=False):
        """Print active jobs
            
           "Active" jobs are those with taskstatus='Incomplete' or 'Check'
        
           Arguments:
             full: If True, print as key:val pair list, If (default) False, print single row summary in 'qstat' style.
             series: If True, print records as groups of auto submitting job series. If (default) False, print in order found.
        """
        print "\n\nTracked:"
        self.curs.execute("SELECT * FROM jobs WHERE taskstatus='Incomplete' OR taskstatus='Check'")
        if not full:
            self.print_header()
        self.print_selected(full=full, series=series)
        print "\n\nUntracked:"
        self.print_untracked(full=full)


# end class JobDB


def complete_job(dbpath=None,jobid=None):
    """Mark the job as 'Complete'
    
       Arguments:
         dbpath: Path to JobDB database. If not given, use default database (see JobDB().__init__)
         jobid: jobid str of job to mark 'Complete'. If not given, uses current job id from the
                environment variable 'PBS_JOBID'
    """
    if jobid == None:
        jobid = misc.job_id()
    db = JobDB(dbpath)
    db.complete_job(jobid)
    db.close()



