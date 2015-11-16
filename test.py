#Ihis script is used for cycle machines to test OMP version
#Input number fo threads varies from 1 to 64 and problem size varies from 128 to 2048
#By: Shaowei

from itertools import product
from subprocess import call
import os
import shlex
import sys



arg_options = [
	[2**x for x in range(7)],
	[2**y for y in range(11, 6, -1)]
]

for args in product(*arg_options):

    cmd_to_run = "./omp -s" + str(args[1]) + " -p" + str(args[0])
    print "========== RUNNING WITH ARGUMENTS: " + str(args) + " =========="
    sys.stdout.flush()
    call(shlex.split(cmd_to_run))
    print "====================" + len(str(args))/2*"=" + " DONE " + len(str(args))/2*"=" + "====================\n"
    sys.stdout.flush()
