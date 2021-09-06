# <STD-HEADER STYLE=TCL ORIG-SRC=SHORE>
# 
#   $Id: random.perf.tcl,v 1.1.2.1 2009/12/03 00:21:13 nhall Exp $
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
#   some procs for timing
#	reading/writing 
#   
proc clearstats {} {
    set dummy [sm gather_stats reset]
}

proc dohot { i f } {
    global hot
    global r
    global body
    global objsize

    if { [expr $i % 2 == 0] } {
	set j [expr $f + [random $hot]]
	sm update_rec $r($j) 0 $body
	return $objsize
    }
    return 0
}

proc time_writes { thid first nobjs nwrites } {
    global id_body_width
    global objsize
    global r
    global commbuf
    link_to_inter_thread_comm_buffer commbuf

    sync

    # set dummy [sm gather_stats reset]
    set tbytes 0
    set wtime [ time {
	sm begin_xct
	    for {set i 0} {$i < $nwrites} {incr i} {
		set j [random $nobjs]
		set j [expr {$first + $j}]
		set start [random $objsize]
		set amt [expr {$objsize - $start} ]
		set amt [random $amt]
		set body [format $id_body_width $amt]
		sm update_rec $r($j) $start $body
		set tbytes [expr {$tbytes + $amt} ]
		set tbytes [expr {$tbytes + [dohot $i $first]} ]
	    }
	sm commit_xct
    } 1 ]
    echo wtime= $wtime
    set sec [lindex $wtime 0]
    set msec [expr {int ($sec * 1000000)} ]
    echo WROTE $nobjs at [expr {$tbytes / $sec}] bytes per second

    for {set i 0} {$i <= $thid} {incr i} { sync }
    set total_msec [lindex $commbuf 0]
    set total_bytes_written [lindex $commbuf 1]
    set total_bytes_read [lindex $commbuf 2]
    incr total_msec $msec
    incr total_bytes_written $tbytes
    set commbuf [list $total_msec $total_bytes_written $total_bytes_read]
    # echo thread $thid has set commbuf to $commbuf
}

proc time_reads { thid first nobjs nreads } {
    global id_body_width
    global objsize
    global r
    global commbuf
    link_to_inter_thread_comm_buffer commbuf

    sync

    # set dummy [sm gather_stats reset]
    set tbytes 0
    set wtime [ time {
	sm begin_xct
	    for {set i 0} {$i < $nreads} {incr i} {
		set j [random $nobjs]
		set j [expr {$first + $j}]
		set start [random $objsize]
		set amt [expr {$objsize - $start} ]
		set amt [random $amt]
		set dummy [sm read_rec $r($j) $start $amt]
		set tbytes [expr {$tbytes + $amt} ]
	    }
	sm commit_xct
    } 1 ]
    set sec [lindex $wtime 0]
    set msec [expr {int ($sec * 1000000)} ]
    echo READ $nobjs at [expr {$tbytes / $sec}] bytes per second


    for {set i 0} {$i < $thid} {incr i} { sync }
    set total_msec [lindex $commbuf 0]
    set total_bytes_written [lindex $commbuf 1]
    set total_bytes_read [lindex $commbuf 2]
    incr total_bytes_read $tbytes
    incr total_msec $msec
    set commbuf [list $total_msec $total_bytes_written $total_bytes_read]
    # echo thread $thid has set commbuf to $commbuf
}

#return total microsecs

proc diff_in_ms { t1 t2 } {
    set s1 [lindex $t1 0]
    set u1 [lindex $t1 1]
    set s2 [lindex $t2 0]
    set u2 [lindex $t2 1]
	
    set seconds [expr { $s2 - $s1 } ]
    set useconds [expr { $u2 - $u1 } ]
    if {$useconds < 0} {
        set useconds [expr { 1000000 + $useconds } ]
	set seconds  [expr { $seconds - 1 } ]
    }
    return [expr {$seconds * 1000000 + $useconds } ]
}

