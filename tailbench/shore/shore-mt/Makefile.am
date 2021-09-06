## Process this file with automake to produce Makefile.in
##  Trouble with 'make install' makes me put all the installed stuff here:
## and let user copy that wherever he chooses.`
include $(top_srcdir)/Makefile.generic
MAINTAINERCLEANFILES = \
	$(installdir)/include/*.h \
	$(installdir)/lib/*.a \
	$(installdir)/bin/* 

CLEANFILES = dox/ warnings
SUBDIRS = src config 
DIST_SUBDIRS = tools $(SUBDIRS) 
EXTRA_DIST = bootstrap Makefile.local Makefile.generic COPYRIGHT-NOTICE README ChangeLog Makefile.in gethugepagesize.sh Doxyfile

## no-op to make this for dist
gethugepagesize.sh: 

dox::
	$(rm) -rf dox/
	mkdir -p dox/
	doxygen
	(cd dox; cp -r html converted)
	(cd dox/html; ../../tools/convert-dox.sh)
