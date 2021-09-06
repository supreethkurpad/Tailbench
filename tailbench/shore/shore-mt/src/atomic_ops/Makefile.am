## Process this file with automake to produce Makefile.in

include $(top_srcdir)/Makefile.generic

EXTRA_DIST =  makelinks.sh
SUBDIRS =  asm_linkage atomic_ops
DIST_SUBDIRS =  $(SUBDIRS)

lib_LIBRARIES     = libatomic_ops.a

include_HEADERS = atomic_ops.h atomic_ops_impl.h

libatomic_ops_a_SOURCES      = atomic_ops.S

atomic_ops.o:  links

links: 
	chmod +x ./makelinks.sh
	./makelinks.sh
