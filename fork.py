#!/usr/bin/env python
"""
input:
    arg[1] a shell program
    arg[2] num of parallel processes
    STDIN, a list of argument for the shell program
output:
    STDOUT, result of arg[2] + each of STDIN
"""
__author__ = 'yangyang'

import os
import sys
from subprocess import Popen, PIPE
from multiprocessing import Pool
from time import sleep

def func(cmd, arg):
    p = Popen([cmd, arg], stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    return p.returncode, arg, out, err

def consume(rslts):
    finished = []
    for rslt in rslts:
        if not rslt.ready():
            continue
        rc, arg, out, err = rslt.get()
        arg = arg.strip()
        if rc == 0:
            out = out.strip() + "\n"
            sys.stdout.write(','.join([str(rc), arg, out]))
        else:
            err = err.strip() + "\n"
            sys.stderr.write(','.join([str(rc), arg, err]))
        finished.append(rslt)
    for rslt in finished:
        rslts.remove(rslt)

if __name__ == '__main__':
    cmd, num = sys.argv[1:3]
    # argv check
    num = int(num)
    pool = Pool(processes=num)
    rslts = set([])
    for arg in sys.stdin:
        rslts.add(pool.apply_async(func, (cmd, arg)))
        if len(rslts) < num:
            continue
        while len(rslts) == num:
            consume(rslts)
            sleep(0.01)
    while len(rslts) > 0:
        consume(rslts)
        sleep(0.01)

