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
            usage: pstat [-h] [-f | -s]
                         [-a | --complete | --continue | --abort | --error ERRMSG]
                         [JOBID [JOBID ...]]
            
            PBS Job Status
            
            positional arguments:
     	      JOBID           Job IDs to query or operate on
            
            optional arguments:
              -h, --help      show this help message and exit
              -f, --full      List all fields instead of summary
              -s, --series    List all fields grouped by continuation jobs
              -a, --all       List all jobs in database, instead of just active jobs
              --complete      Mark jobs as 'Complete'
              --continue      Re-submit auto jobs. By default, re-submit all
              --abort         Abort jobs
              --error ERRMSG  Add error message for jobs that failed
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
            usage: taskmaster [-h] [-d DELAY] [-k]
            
            Automatically resubmit PBS jobs
            
            optional arguments:
              -h, --help            show this help message and exit
              -d DELAY, --delay DELAY
                                    How long to delay ("[[[DD:]HH:]MM:]SS") between
                                    executions. Default is "15:00".
              -k, --kill            Kill the currently running taskmaster
```
        
The 'taskmaster' script periodically monitors and re-submits 'auto' jobs as necessary until they are "Complete". It does this by executing ```pstat --continue```, and then submitting a PBS job that will execute 'taskmaster' with the same arguments but only becomes eligible DELAY about of time into the future. In this fastion it will continually monitor and re-submit 'auto' jobs until the finish. 'taskmaster' continues re-submitting itself until ```taskmaster --kill``` is executed.
        
'taskmaster' submits itself on the prismsprojectdebug_flux queue on flux, and would have to be edited to run on other systems. The default DELAY is 15:00 (15 minutes).

