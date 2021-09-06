#!/usr/bin/python

import os, sys

def cmd(c): return os.popen(c).read()

cmd("mkdir -p libs")
for line in cmd("ldd " + sys.argv[1]).split("\n"):
    x = line.split("=>")
    if len(x) != 2: continue
    lib = x[0].rstrip().lstrip()
    path = x[1].split("(")[0].lstrip().rstrip()
    if len(path) == 0: continue # vdso..
    print "Copying", lib, path, cmd("cp %s libs/" % path)
print "Done"




