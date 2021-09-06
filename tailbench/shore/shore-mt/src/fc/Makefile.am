## Process this file with automake to produce Makefile.in
SUBDIRS = tests
include $(top_srcdir)/Makefile.generic
EXTRA_DIST = fc_error.dat

lib_LIBRARIES     = libfc.a

FC_ERROR_GENFILES_H  = fc_errmsg_gen.h \
             fc_einfo_gen.h \
             fc_error_def_gen.h \
             fc_error_enum_gen.h \
             fc_einfo_bakw_gen.h

GENFILES = $(FC_ERROR_GENFILES_H)

include_HEADERS = \
    atomic_container.h \
    atomic_templates.h \
    block_alloc.h \
    cpu_info.h \
    dynarray.h \
    mem_block.h \
    errlog.h errlog_s.h \
    gethrtime.h \
    largefile_aware.h \
    nbox.h \
    os_types.h \
    rand48.h \
    stime.h \
    tls.h \
    vtable.h \
    valgrind_help.h \
    w.h w_autodel.h w_base.h w_bitmap.h w_bitvector.h \
    w_compat_strstream.h \
    w_hashing.h \
    w_debug.h w_defines.h \
    w_error.h \
    w_findprime.h \
    w_getopt.h w_gettimeofday.h \
    w_hash.h w_heap.h w_list.h w_minmax.h \
    w_rc.h w_rusage.h \
    w_stream.h w_strstream.h \
    w_stat.h \
    w_workaround.h \
    $(GENFILES)


libfc_a_SOURCES      = \
        rand48.cpp \
        block_alloc.cpp \
	cpu_info.cpp fc-noinline.cpp \
        dynarray.cpp \
        mem_block.cpp \
        gethrtime.cpp \
        w_compat_strstream.cpp \
        w_form.cpp \
        common_tmpl.cpp w_debug.cpp errlog.cpp w_getopt.cpp nbox.cpp \
        stime.cpp vtable.cpp w_base.cpp w_bitmap.cpp w_error.cpp \
        valgrind_help.cpp \
        w_listm.cpp w_rc.cpp \
        w_input.cpp tls.cpp w_findprime.cpp

MOSTLYCLEANFILES = $(GENFILES)

debug.$(OBJEXT): $(FC_ERROR_GENFILES_H)
$(FC_ERROR_GENFILES_H): fc_error.dat
	$(PERL) $(top_srcdir)/tools/errors.pl -d -e $?

