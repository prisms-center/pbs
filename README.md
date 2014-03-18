pbs
===

A python package for submitting and managing PBS jobs


### Summary

When submitted through this package or the included scripts, PBS jobs are stored in a SQLite jobs database. This allows for convenient monitoring and searching of submitted jobs. Jobs marked as 'auto' can be automatically or easily re-submitted until they reach completion. 
    
Jobs are marked 'auto' either by submitting through the python class pbs.Job with the attribute ```auto=True```, or by submitting a PBS script which contains the line ```#auto=True``` using the included 'psub' script. 

Jobs can be monitored using 'pstat', which is similar to qstat. All 'auto' jobs which have stopped can be resubmitted using ```pstat --continue```. 

Example screen shot:
```
$ pstat


Tracked:
JobID        JobName                  Nodes Procs     Walltime S      Runtime Task                     A ContJobID   
------------ ------------------------ ----- ----- ------------ - ------------ ------------------------ - ------------
11791024     STDIN                      1     1     0:01:00:00 Q            - Incomplete               1 -           
11791025     STDIN                      1     1     0:01:00:00 Q            - Incomplete               1 -           


Untracked:
JobID        JobName                  Nodes Procs     Walltime S      Runtime Task                     A ContJobID   
------------ ------------------------ ----- ----- ------------ - ------------ ------------------------ - ------------
11791026     taskmaster                 1     1     0:01:00:00 W   0:01:00:00 Untracked                0 -           
```
    
Additionally, when scheduling periodic jobs is not allowed other ways, the 'taskmaster' script can fully automate this process. 'taskmaster' executes ```pstat --continue``` and then re-submits itself to execute again periodically.

A script marked 'auto' should check itself for completion and when reached execute ```pstat --continue $PBS_JOBID``` in bash, or ```pbs.complete_job()``` in python. If an 'auto' job script does not set it's taskstatus to "Complete" it may continue to be re-submitted over and over.

Jobs not marked 'auto' are shown with the status "Check" in 'pstat' until the user marks them as "Complete".

### Contains

* **pbs**: python package
* **pstat**: python script to interact with the jobs database, similar to qstat
* **psum**: python script to submit jobs and add them to the jobs database, similar to qsub
* **taskmaster**: python script that periodically checks and re-submits 'auto' jobs

### Installation

* Testing has been done using python2.7.5
* The 'pbs' module can be installed by placing it in your PYTHONPATH.
* The scripts 'pstat', 'psub', and 'taskmaster' can be installed by placing in your PATH.
* The scripts require the module 'argparse'.
* On flux, you can use my installation by including ```/scratch/prismsproject_flux/bpuchala/Public/pythonmodules``` in your PYTHONPATH, and ```/scratch/prismsproject_flux/bpuchala/Public/scripts``` in your PATH. It may be necessary to also run ```module load python/2.7.5```. This could be placed in your ```.bash_profile```.

### Documentation:

The python package pbs is documented python-style. Documentation can be viewed from the python interactive shell with: ```help('pbs')```, ```help('pbs.job')```, etc.

The python scripts 'pstat', 'psub', and 'taskmaster' have usage information that can be viewed with the ```-h``` or ```--help``` options.

### Description
#### pbs: 
Contains 4 submodules: 

* **pbs.misc**: Contains functions for interacting with qstat, qsub, and qdel. Also contains functions for common conversion and environment variables.

* **pbs.jobdb**: Contains the JobDB class, which allows interactions with an SQLite database containing records of PBS jobs. 
* **pbs.job**: Contains the Job class, which contains all the settings for a particular job (nodes requsted, walltime, command to run, etc.).
* **pbs.templates**: Specialized submodule for PRISMS on flux, but provides an example of how to create templates for particular types of Jobs. Contains templates for creating Job objects that areappropriate for PRISMS jobs, Non-PRISMS jobs, PRISMS debug queue jobs, and PRISMS special request jobs.

#### pstat:
From ```pstat -h```:
```            
usage: pstat [-h] [-f | -s] [-a | --range MINID MAXID | --recent DD:HH:MM:SS]
             [--active]
             [--complete | --continue | --reset | --abort | --error ERRMSG | --delete | --key KEY]
             [--force]
             [JOBID [JOBID ...]]

Print or modify PBS job and task status.

By default, 'pstat' prints status for select jobs. Jobs are 
selected by listing jobids or using --all, --range, or 
--recent, optionally combined with --active. Running 'pstat'
with no selection is equivalent to selecting '--all --active'. 
The default display style is a summary list. Other options are
--full or --series.

Using one of --complete, --continue, --error, --abort, or 
--delete modifies status instead of printing. User 
confirmation is required before a modification is applied,
unless the --force option is given.

Job status is as given by PBS for a single PBS job ('C', 'R', 
'Q', etc.).

Task status is user-defined and defines the status of a single
PBS job within a possible series of jobs comprising some task. 
'Auto' jobs may be re-submitted with the --continue option. 
Please see: 
             https://github.com/prisms-center/pbs
for more information about 'auto' jobs.

Possible values for task status are:
  
  "Complete":    Job and task are complete.
  
  "Incomplete":  Job or task are incomplete.
  
  "Continued":   Job is complete, but task was not complete. In
                 this case, 'continuation_jobid' is set with 
                 the jobid for the next job in the series of 
                 jobs comprising some task.
  
  "Check":       Non-auto job is complete and requires user 
                 input for status. 
  
  "Error:.*":    Some kind of error was noted.
  
  "Aborted":     The job and task have been aborted.

positional arguments:
  JOBID                 Job IDs to query or operate on

optional arguments:
  -h, --help            show this help message and exit
  -f, --full            List all fields instead of summary
  -s, --series          List all fields grouped by continuation jobs
  -a, --all             Select all jobs in database
  --range MINID MAXID   A range of Job IDs (inclusive) to query or operate on
  --recent DD:HH:MM:SS  Select jobs created or modified within given amout of time
  --active              Select active jobs only. May be combined with --range and --recent
  --complete            Mark jobs as 'Complete'
  --continue            Re-submit auto jobs
  --reset               Mark job as 'Incomplete'
  --abort               Call qdel on job and mark as 'Aborted'
  --error ERRMSG        Add error message.
  --delete              Delete jobs from database. Aborts jobs that are still running.
  --key KEY             Output data corresponding to 'key' for selected jobs.
  --force               Modify jobs without user confirmation
```

```pstat``` can be used to make additional shortcut functions.  For example, the following can be added to your ```.bash_profile``` or ```.bashrc``` file to make it easy to jump to a job's run directory:
```
# pgo: cd to the rundir of a job                                                                            
#   usage:  `pgo jobid`                                                       
#                                                                 
pgo() {
    cd $(pstat --key rundir $1 | cut -d " " -f 2)
}

```

#### psub:
From ```psub -h```:
```            
        usage: psub PBS_SCRIPT
```

Submits a PBS script and adds it to the jobs database so that it will be tracked with pstat. This submits the exact PBS_SCRIPT, and attempts to parse it for fields that are stored in the database. 

If the script contains a line which matches the python regex ```"auto=\s*(.*)\s"```, where the group ```(.*)``` matches ```"[tT](rue)*|1"```, then the job is considered an 'auto' job and can be automatically re-submitted using 'pstat' or 'taskmaster' until it is marked with the taskstatus "Complete". This means the script should check itself for completion and when reached execute ```pstat --continue $PBS_JOBID``` in bash, or ```pbs.complete_job()``` in python. If an 'auto' job script does not set it's taskstatus to "Complete" it may continue to be re-submitted over and over.

#### taskmaster:
From ```taskmaster -h```:
```            
usage: taskmaster [-h] [-d DELAY] [--hold | --release | --kill]

Automatically resubmit PBS jobs

optional arguments:
  -h, --help            show this help message and exit
  -d DELAY, --delay DELAY
                        How long to delay ("[[[DD:]HH:]MM:]SS") between
                        executions. Default is "15:00".
  --hold                Place a hold on the currently running taskmaster
  --release             Release the currently running taskmaster
  --kill                Kill the currently running taskmaster
```
        
The 'taskmaster' script periodically monitors and re-submits 'auto' jobs as necessary until they are "Complete". It does this by executing ```pstat --continue```, and then submitting a PBS job that will execute 'taskmaster' with the same arguments but only becomes eligible DELAY about of time into the future. In this fastion it will continually monitor and re-submit 'auto' jobs until the finish. 'taskmaster' continues re-submitting itself until ```taskmaster --kill``` is executed.
        
'taskmaster' submits itself on the prismsprojectdebug_flux queue on flux, and would have to be edited to run on other systems. The default DELAY is 15:00 (15 minutes).

