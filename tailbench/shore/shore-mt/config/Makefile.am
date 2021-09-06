## needed for "make dist" also
include $(top_srcdir)/Makefile.generic
EXTRA_DIST = shore.def
include_HEADERS = shore.def shore-config.h
MAINTAINERCLEANFILES = shore-config.h

INDEPENDENT_MAKEFLAGS   = $(AM_CXXFLAGS)

all-local:
	echo "These are the CXX flags used with the last build: " > $(top_srcdir)/makeflags
	echo $(INDEPENDENT_MAKEFLAGS) >> $(top_srcdir)/makeflags

install-exec-local:
	echo "These are the CXX flags used with the build for this installation: " > $(includedir)/makeflags
	echo $(INDEPENDENT_MAKEFLAGS) >> $(includedir)/makeflags
	cp shore.def $(includedir)/
	cp shore-config.h $(includedir)/

uninstall-local:
	rm -f $(includedir)/makeflags 

distclean-local:
	echo "Nothing to do for distclean in config/"
