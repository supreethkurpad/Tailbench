# <STD-HEADER STYLE=TCL ORIG-SRC=SHORE>
# 
#   $Id: trans.tcl,v 1.1.2.4 2010/03/25 18:05:32 nhall Exp $
# 
# 
# 
#  This file was a part of the SHORE database system distribution.
#  This file has been modified for use with this database system.
#  The original copyright notice for SHORE Software appears below. 
# 
# SHORE -- Scalable Heterogeneous Object REpository
# 
# Copyright (c) 1994-97 Computer Sciences Department, University of
#                       Wisconsin -- Madison
# All Rights Reserved.
# 
# Permission to use, copy, modify and distribute this software and its
# documentation is hereby granted, provided that both the copyright
# notice and this permission notice appear in all copies of the
# software, derivative works or modified versions, and any portions
# thereof, and that both notices appear in supporting documentation.
# 
# THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
# OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
# "AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
# FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.
# 
# This software was developed with support by the Advanced Research
# Project Agency, ARPA order number 018 (formerly 8230), monitored by
# the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
# 
#   -- do not edit anything above this line --   </STD-HEADER>

set_config_info
set docheckpoint 0

source $script_dir/vol.init
proc scanit { f } {
    set scan [sm scan_file_create $f t_cc_record]
    set pin [sm scan_file_next $scan 0]
    set i 0
    while {$pin != "NULL"} {
        set rec [sm pin_rid $pin]
        # do NOT unpin -- it blows the context of the scan
        # sm pin_unpin $pin
        set pin [sm scan_file_next $scan 0]
        set i [expr {$i+1} ]
    }
    sm scan_file_destroy $scan
    return $i 
}

proc random_restart { a } {
    global verbose_flag
    verbose random_restart $a
    # detach this thread from any tx 
    set tx [sm xct]
    if {[is_set tx] && $tx != 0 } {
        sm detach_xct $tx
    }
    if {$verbose_flag} {
        verbose lock table BEFORE restart:
        sm dump_locks
    }
    set j [random 3]
    verbose restarting $j times ...
    if { $j <= 0 } {
        set j 1
    }
    verbose restarting $j times ...

    for { set i 0 } { $i <= $j } { incr i } {
        _restart $a
    }
    if {$verbose_flag} {
        verbose lock table AFTER restart: 
        sm dump_locks
    }
}

proc rtrans { clean action finish } {
    global f0 docheckpoint nrecs nrecsnow page_size formsize
    verbose 
    verbose
    verbose starting: rtrans clean $clean action $action finish $finish docheckpoint $docheckpoint
    # set formsize [expr {$page_size - 10}]
    sm begin_xct
    set tx [sm xct]
    set tid [sm xct_to_tid $tx]
    verbose  $tid
    set nrecs [scanit $f0]

    if { $action == "readonly" } {
        set nrecsnow $nrecs
    }
    if { $action == "readwrite" } {
        if { 1 } {
            set form %0${page_size}d
            set rec [sm create_rec $f0 hdr 1000 [formatlong 0 $formsize %010d 10] ]
            set nrecsnow [expr {$nrecs + 1}]
            # assert {expr [scanit $f0] == $nrecsnow}
        }
        if { 0 } {
           for { set j 1 } { $j < 1535 } { incr j } {
                set rec [sm create_rec $f0 hdr 1 "1" ]
                # grab some extent locks - in an attempt to reproduce
                # a bug calling log_xct_prepare_alk
                set id [format "x(1.%d)" $j]
                sm lock $id IX LONG 0
           }
           set nrecsnow [expr {$nrecs + $j}]
        }
    }
    verbose before switch $finish
    case $finish in {
        { "commit.restart" } {
            sm commit_xct
        } 
        { "abort.restart" } {
            sm abort_xct
            set nrecsnow $nrecs
        } 
        { "extern.abort.restart" } {
            sm enter2pc $finish.$action.$clean
            sm set_coordinator none
            sm abort_xct
            set nrecsnow $nrecs
        } 
        { "extern.prepare.commit.restart" } {
            sm enter2pc $finish.$action.$clean
            sm set_coordinator none
            if {$docheckpoint == 1} { sm checkpoint }
            set vote [sm prepare_xct]
            verbose vote=$vote
            if {$vote != 1} {
                   verbose committing
                   sm commit_xct
            } else {
                        set t [sm xct]
                    assert {expr $t==0}
                    verbose already committed
            }
                verbose test done
        } 
        { "extern.prepare.abort.restart" } {
            sm enter2pc $finish.$action.$clean
            sm set_coordinator none
            # just to test fuzzy ckpts
            if {$docheckpoint == 1} { sm checkpoint }
            set vote [sm prepare_xct]
            verbose vote=$vote
            if {$vote != 1} {
                sm abort_xct
            } else {
                set t [sm xct]
                assert {expr $t==0}
                verbose aborted/readonly
            }
            set nrecsnow $nrecs
        } 
        { "extern.restart" } {
           # recover, should not be found
            sm enter2pc $finish.$action.$clean
            sm set_coordinator none
            verbose restarting clean=$clean...
            random_restart $clean
            catch {sm recover2pc $finish.$action.$clean} catcherr
            verbose EXPECT error (no such prepared tx): $catcherr
            # aborted so restore nrecsnow
            set nrecsnow $nrecs
        }
        { "extern.prepare.restart.commit" } {
           # recover, find, commit (unless readonly)
            sm enter2pc $finish.$action.$clean
            sm set_coordinator none
            if {$docheckpoint == 1} { sm checkpoint }
            set vote [sm prepare_xct]
            verbose vote=$vote
            verbose random restart $clean ...
            random_restart $clean
            verbose restarted at $finish
            if { $action == "readonly" }  {
                verbose SHOULD GET ERROR: no such prepared tx
                catch {sm recover2pc $finish.$action.$clean} catcherr
                verbose EXPECT error (no such prepared tx) : $catcherr
                # Now, if this were in fact
                # distributed, we'd expect this
                # to abort because one was readonly
                # and therefore indistinguishable
                # from an aborted tx
            } else {
				verbose calling recover2pc $finish.$action.$clean
                set t [sm recover2pc $finish.$action.$clean]
                verbose recovered $t 
                assert {expr $t != 0}
                # it should be attached
                verbose $tid $t
                assert {expr [string compare $tid $t] == 0}
                sm commit_xct
            }
        }
        { "extern.prepare.restart.abort" } {
           # recover, find, abort
            sm enter2pc $finish.$action.$clean
            sm set_coordinator none
            if {$docheckpoint == 1} { sm checkpoint }
            set vote [sm prepare_xct]
            verbose vote=$vote
            verbose restarting...
            random_restart $clean
            if { $action == "readonly" }  {
                verbose SHOULD GET ERROR: no such prepared tx
                catch {sm recover2pc $finish.$action.$clean} catcherr
                verbose EXPECT error (no such prepared tx): $catcherr
            } else {
                set t [sm recover2pc $finish.$action.$clean]
                assert {expr $t != 0}
                # it should be attached
                verbose $tid $t
                assert {expr [string compare $tid $t] == 0}
                sm abort_xct
            }
            set nrecsnow $nrecs
        } 
    }
    verbose after switch
    verbose restarting...
    random_restart $clean

    sm begin_xct
    # assert {expr [string compare [scanit $f0] $nrecsnow]==0}
    sm commit_xct
    verbose finished with: rtrans $clean $action $finish
}

proc trans { action finish } {
    global f0 docheckpoint nrecsnow page_size
    verbose 
    verbose
	# no clean
    verbose starting: trans $action $finish docheckpoint=$docheckpoint
    set formsize [expr {$page_size - 10}]
    sm begin_xct
    set tx [sm xct]
    set tid [sm xct_to_tid $tx]
    verbose $tid

    set nrecs [scanit $f0]
    if { $action == "readonly" } {
        set nrecsnow $nrecs
    }
    if { $action == "readwrite" } {
        set rec [sm create_rec $f0 hdr 1000 [formatlong 0 $formsize %010d 10] ]
        set nrecsnow [expr {$nrecs + 1}]
		assert {expr [scanit $f0] == $nrecsnow}
    }
    case $finish in {
        { "restart.dirty" } {
                # detach this thread from any tx 
                if {$tx != 0 } {
                        sm detach_xct $tx
                }
				# it will abort: restor nrecsnow
				set nrecsnow $nrecs
                restart 
        }
        { "restart.clean" } {
                # detach this thread from any tx 
                if {$tx != 0 } {
                        sm detach_xct $tx
                }
				# it will abort: restor nrecsnow
				set nrecsnow $nrecs
                cleanrestart 
        } 
        { "commit" } {
            sm commit_xct
        } 
        { "abort" } {
            sm abort_xct
            set nrecsnow $nrecs
        } 
        { "extern.commit" } {
            sm enter2pc "glarch.$finish.$action"
            sm set_coordinator none
            verbose SHOULD GET ERROR: not prepared
            catch {sm commit_xct } catcherr
            verbose EXPECT error (not prepared): $catcherr
            verbose explicitly aborting ...
            sm abort_xct
            set nrecsnow $nrecs
        } 
        { "extern.abort" } {
            sm enter2pc "glarch.$finish.$action"
            sm set_coordinator none
            sm abort_xct
            set nrecsnow $nrecs
        } 
        { "extern.prepare.commit" } {
            sm enter2pc "glarch.$finish.$action"
            sm set_coordinator none
            if {$docheckpoint == 1} { sm checkpoint }
            set vote [sm prepare_xct]
            verbose vote=$vote
            if {$vote != 1} {
                sm commit_xct
            } else {
                set t [sm xct]
                assert {expr $t==0}
            }
            verbose committed 
        } 
        { "extern.prepare.abort" } {
            sm enter2pc "glarch.$finish.$action"
            sm set_coordinator none
            if {$docheckpoint == 1} { sm checkpoint }
            set vote [sm prepare_xct]
            verbose vote=$vote
            if {$vote != 1} {
                sm abort_xct
                verbose aborted
            } else {
                set t [sm xct]
                assert {expr $t==0}
                verbose read-only aborted
            }
            set nrecsnow $nrecs
        } 
        { "prepare.commit" } {
            verbose SHOULD GET ERROR: not participating in external 2pc
            set nrecsnow $nrecs
            if {$docheckpoint == 1} { sm checkpoint }
            set err [catch {sm prepare_xct} vote]
            if {$err == 0} {
                # didn't get error
                assert {expr 1==0}
            } else {
                    verbose EXPECT error (not participating in 2pc): $vote
            }
            verbose explicitly aborting...
            sm abort_xct
        } 
        { "prepare.abort" } {
            verbose SHOULD GET ERROR: not participating in external 2pc
            set nrecsnow $nrecs
            if {$docheckpoint == 1} { sm checkpoint }
            if {[catch {sm prepare_xct} vote] == 0} {
                # didn't get error
                assert {expr 1==0}
            } else {
                    verbose EXPECT error (not participating in 2pc): $vote
            }
            verbose explicitly aborting...
            sm abort_xct
        }
    }
    sm begin_xct
	verbose nrecsnow $nrecsnow
    assert {expr [string compare [scanit $f0] $nrecsnow]==0}
    sm commit_xct
    verbose  finished with  trans $action $finish
}
set actionlist { readonly readwrite }

if [is_set do_file_create] {
    sm begin_xct
    set f0 [sm create_file $volid]]
    scanit $f0
    sm commit_xct
}

