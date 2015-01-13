import job, misc
import sys

def PrismsJob( name = "STDIN", \
               nodes = "1", \
               ppn = "16", \
               walltime = "1:00:00", \
               pmem = "3800mb", \
               exetime = None, \
               message = "a", \
               email = None, \
               command = None, \
               auto = False):
    """
    Returns a Job for the normal PRISMS queue.
    
    Normal PRISMS-related jobs are limited to 62 nodes (1000 cores), 
    and 48 hrs of walltime. They are given priority -200.
    """
    
    if int(nodes)*int(ppn) > 1000:
        print "Error in PrismsJob(). Requested more than 1000 cores."
        sys.exit()
    
    if int(ppn) > 16:
        print "Error in PrismsJob(). Requested more than 16 ppn."
        sys.exit()
    
    if misc.hours(walltime) > 48.0:
        print "Error in PrismsJob(). Requested more than 48 hrs walltime."
        sys.exit()
    
    j = job.Job( name = name, \
             account = "prismsproject_fluxoe", \
             nodes = nodes, \
             ppn = ppn, \
             walltime = walltime, \
             pmem = pmem, \
             qos = "flux", \
             queue = "fluxoe", \
             exetime = exetime, \
             message = message, \
             email = email, \
             priority = "-200", \
             command = command, \
             auto = auto)
    
    return j

def NonPrismsJob( name = "STDIN", \
               nodes = "1", \
               ppn = "16", \
               walltime = "1:00:00", \
               pmem = "3800mb", \
               exetime = None, \
               message = "a", \
               email = None, \
               command = None, \
               auto = False):
    """
    Returns a non-PRISMS-related Job for the normal PRISMS queue.
    
    Normal non-PRISMS-related jobs are limited to 62 nodes (1000 cores), 
    and 48 hrs of walltime. They are given priority -1000.
    """
    
    if int(nodes)*int(ppn) > 1000:
        print "Error in NonPrismsJob(). Requested more than 1000 cores."
        sys.exit()
    
    if int(ppn) > 16:
        print "Error in NonPrismsJob(). Requested more than 16 ppn."
        sys.exit()
    
    if misc.hours(walltime) > 48.0:
        print "Error in NonPrismsJob(). Requested more than 48 hrs walltime."
        sys.exit()
    
    j = job.Job( name = name, \
             account = "prismsproject_fluxoe", \
             nodes = nodes, \
             ppn = ppn, \
             walltime = walltime, \
             pmem = pmem, \
             qos = "flux", \
             queue = "fluxoe", \
             exetime = exetime, \
             message = message, \
             email = email, \
             priority = "-1000", \
             command = command, \
             auto = auto)
    
    return j

def PrismsPriorityJob( name = "STDIN", \
               nodes = "1", \
               ppn = "16", \
               walltime = "1:00:00", \
               pmem = "3800mb", \
               exetime = None, \
               message = "a", \
               email = None, \
               command = None, \
               auto = False):
    """
    Returns a high-priority Job for the normal PRISMS queue.
    
    Normal PRISMS-related jobs are limited to 62 nodes (1000 cores), 
    and 48 hrs of walltime. They are given priority 0.
    """
    
    if int(nodes)*int(ppn) > 1000:
        print "Error in PrismsPriorityJob(). Requested more than 1000 cores."
        sys.exit()
    
    if int(ppn) > 16:
        print "Error in PrismsPriorityJob(). Requested more than 16 ppn."
        sys.exit()
    
    if misc.hours(walltime) > 48.0:
        print "Error in PrismsPriorityJob(). Requested more than 48 hrs walltime."
        sys.exit()
    
    j = job.Job( name = name, \
             account = "prismsproject_fluxoe", \
             nodes = nodes, \
             ppn = ppn, \
             walltime = walltime, \
             pmem = pmem, \
             qos = "flux", \
             queue = "fluxoe", \
             exetime = exetime, \
             message = message, \
             email = email, \
             priority = "0", \
             command = command, \
             auto = auto)
    
    return j
             
def PrismsDebugJob( name = "STDIN", \
               nodes = "1", \
               ppn = "16", \
               walltime = "1:00:00", \
               pmem = "3800mb", \
               exetime = None, \
               message = "a", \
               email = None, \
               command = None, \
               auto = False):
    """
    Returns a Job for the debug queue.
    
    The debug queue has 5 nodes (80 cores), and a 6 hr walltime limit.
    """
    
    if int(nodes)*int(ppn) > 80:
        print "Error in PrismsDebugJob(). Requested more than 80 cores."
        sys.exit()
    
    if int(ppn) > 16:
        print "Error in PrismsDebugJob(). Requested more than 16 ppn."
        sys.exit()
    
    if misc.hours(walltime) > 6.0:
        print "Error in PrismsDebugJob(). Requested more than 6 hrs walltime."
        sys.exit()
    
    j = job.Job( name = name, \
             account = "prismsprojectdebug_fluxoe", \
             nodes = nodes, \
             ppn = ppn, \
             walltime = walltime, \
             pmem = pmem, \
             qos = "flux", \
             queue = "fluxoe", \
             exetime = exetime, \
             message = message, \
             email = email, \
             priority = "-1000", \
             command = command, \
             auto = auto)
    
    return j

def PrismsSpecialJob( name = "STDIN", \
               nodes = "1", \
               ppn = "16", \
               walltime = "1:00:00", \
               pmem = "3800mb", \
               exetime = None, \
               message = "a", \
               email = None, \
               command = None, \
               auto = False):
    """
    Returns a special request Job for the normal PRISMS queue.
    
    Normal PRISMS-related jobs are limited to 62 nodes (1000 cores), 
    and 48 hrs of walltime.
    
    When more than 1000 cores or 48 hrs walltime is required, please
       email prismsproject.fluxadmin@umich.edu to make a special request.
    """
    
    if int(ppn) > 16:
        print "Error in PrismsPriorityJob(). Requested more than 16 ppn."
        sys.exit()
    
    j = job.Job( name = name, \
             account = "prismsproject_fluxoe", \
             nodes = nodes, \
             ppn = ppn, \
             walltime = walltime, \
             pmem = pmem, \
             qos = "flux", \
             queue = "fluxoe", \
             exetime = exetime, \
             message = message, \
             email = email, \
             priority = "-200", \
             command = command, \
             auto = auto)
    
    return j
    

