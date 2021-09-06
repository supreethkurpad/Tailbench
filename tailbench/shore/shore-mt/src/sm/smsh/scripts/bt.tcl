# <STD-HEADER STYLE=TCL ORIG-SRC=SHORE>
# 
#   $Id: bt.tcl,v 1.3 2010/06/08 22:28:28 nhall Exp $
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

#
# UNDER CONSTRUCTION
#

# use max_btree_entry_size variable, which is set by set_config_info
set_config_info
set maxnum $max_btree_entry_size
# verbose  max_btree_entry_size $maxnum

set unique uni_btree

proc mkval { e } {
    verbose make value "000...0a" of length $e 
    set res [format "%0*s" $e a]
	# verbose value is $res
    return $res
}

proc mkkey { k l } {
    verbose make key from $k 
    set res [format "%s%0*s" $k $l ""]
	verbose key is $res
    return $res
}

proc middle  { a b } {
    set m [expr $a + $b]
    set m [expr $m / 2]
    set c [ascii_char $m]
    return $c
}


# 
# insert values in list
#
# arguments:
#   ndx -- index id (global)
#   uniq -- 1 if unique, 0 otherwise (global)
#   klen -- key size : s/m/l/v for small,med,large,vbl
#   elen -- element size :  s/l/v
#           small, large, varying
#   nsame -- number in sequence of this operation
#		with same key
#   op --   i/d/s/c insert, delete, search, combination
#   term --   a/c/r  abort, commit, restart
#

proc doop { op nsame klen elen term } {
    global ndx
    global uniq
    global keylist
    global maxnum

    set i 0
    set listlen [llength $keylist]

    verbose "{" sm begin_xct
    sm begin_xct

    while {$i < $listlen} {
	# figure out key length
	# and value length
	switch $klen {
	    s { set kl 0 }
	    v { set kl $i }
	    m { set kl [expr $maxnum / 2] }
	    l { set kl $maxnum }
	    default { 
		verbose bad value for kl: $klen 
		assert {0}
	    }
	}
	set key [mkkey [lindex $keylist $i] $kl]
	# adjust kl to reflect actual length of key
	set kl [string length $key]
	verbose KEY: klen $klen kl $kl

	switch $elen {
	    s { set el 0 }
	    v { set el $i
	       if [expr $el > $maxnum / 2] {
			   set el [expr $maxnum / 2]
	       }
	    }
	    m { set el [expr $maxnum / 2 - $kl] }
	    l { set el [expr $maxnum - $kl] }
	    default {
		verbose bad value for el: $elen 
		assert {0}
	    }
	}
	verbose ELEMENT elen $elen el $el maxnum: $maxnum
	set value [mkval $el]
	verbose $key length of value is [string length $value]

	for {set k 0} {$k < $nsame} {incr k} {
	    switch  $op  {
		insert {
		    verbose OP $op sm create_assoc $ndx $key (el=$el elen=$elen)
		    set caught [catch {sm create_assoc $ndx $key $value } catcherr]
		    verbose $catcherr
			if {$caught == 0} {
				verbose ok
			} else {
				if [error_is $catcherr E_DUPLICATE] {
				   verbose ERROR: $catcherr ( uniq= $uniq ) (OK)
				   break
				}
				if [error_is $catcherr E_1PAGESTORELIMIT] {
					# same as E_RECWONTFIT for 1-page btree
					assert {0}
				}
			}
		}
		
		remove {
		    verbose OP $op sm destroy_assoc $ndx $key <value>
		    sm destroy_assoc $ndx $key $value
		}
		search {
		    verbose OP $op sm find_assoc $ndx $key 
		    sm find_assoc $ndx $key $value
		}
		combo {
		    verbose OP $op sm create_assoc $ndx $key (elen= $elen)
		    catch {sm create_assoc $ndx $key $value } catcherr

		    verbose OP $op sm find_assoc $ndx $key 
		    sm find_assoc $ndx $key $value

		    verbose OP $op sm destroy_assoc $ndx $key 
		    sm destroy_assoc $ndx $key $value
		}
		default {
		    verbose bad value for op: $op
		    assert {0}
		}
	    }
	    incr k
	}

	# gather stats, see if we are done

	incr i
    }

    switch $term {
	commit {
	    verbose sm commit_xct
	    sm commit_xct
	} 
	abort {
	    if {$logging==0} {
		verbose "Logging off: sm abort_xct not done.  Committing instead."
		sm commit_xct 
	    } else {
		verbose sm abort_xct 
		sm abort_xct 
	    }
	} 
	restart.clean {
	    verbose random_restart true
	    random_restart true
	}
	restart {
	    verbose random_restart false
	    random_restart false
	}
	default {
	    verbose bad value for term: $term
	    assert {0}
		
	}
    }
	verbose "}"
}

# repeat op until
# given stat is reached
# interesting stats include
#   u_long bt_splits		Btree pages split (interior and leaf)
#   u_long bt_cuts		Btree pages removed (interior and leaf)
#   u_long bt_grows		Btree grew a level
#   u_long bt_shrinks		Btree shrunk a level

proc do_until_stat { stat val op nsame klen elen term } {
   verbose do_until_stat op= $op until $stat == $val
   verbose nsame $nsame
   verbose klen $klen
   verbose elen $elen
   verbose term $term

   clearstats

   while {1} {
       verbose calling doop $op $nsame $klen $elen $term 
       doop $op $nsame $klen $elen $term 

       verbose sm gather_stats 
       set stats [sm gather_stats ]
       set wanted [select_stat $stats $stat]
       verbose wanted $wanted
       set total [lindex $wanted 1]
       verbose $stat is now $total.  Wanted $val
       if [expr $total >= $val] break;
   }
}

proc test_scan {ndx nrec} {
    global volid
    dstats $volid
    set scan [sm create_scan $ndx >= neg_inf <= pos_inf]
    set res {}
    for {set i 0} {$i < $nrec} {incr i} {
		set r [sm scan_next $scan]
		if {$r == "eof"} then { break }
		set key [string trimleft [lindex $r 0] 0]
		set el [lindex $r 1]
		set ellength [string length $el]
		verbose "$i: scanned ($key, <$ellength chars>)"
		lappend res $key
    }
    verbose done with scan of $ndx,  $i items, expected $nrec
    assert {expr [string compare [sm scan_next $scan] eof]==0}
    assert {expr $i >= $nrec}
    sm destroy_scan $scan
    verbose "-- successful scan: " 
    return $res
}

