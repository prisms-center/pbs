# test script for pbs python package
# on flux add '/scratch/prismsproject_flux/bpuchala/Public/pythonmodules' to your PYTHONPATH
# inside python see usage with:
#  help("pbs"), help("pbs.job"), and help("pbs.templates")

import pbs

# create a PBS Job appropriate for the prismsprojectdebug_flux queue
j = pbs.PrismsDebugJob(nodes="2", command="echo \"hello\" > test.txt")

# other options are: PrismsJob(), NonPrismsJob(), PrismsSpecialJob() and PrismsPriorityJob()

# take a look at the qsub script associated with the Job
print j.qsub_string()

# if you want to write a bash submit script file
j.script("submit.sh")

# or just submit the job from python
j.submit()
