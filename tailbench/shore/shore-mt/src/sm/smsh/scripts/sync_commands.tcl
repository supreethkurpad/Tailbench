#
# Old sync commands:
# sync [comment] (that way when one of these is waiting, you can tell
#                 which sync command it's waiting on)
# sync_thread t1 t2 t3 ...
#                in sequence, it issues syncs with these threads.
#                No comment.
#                Order is important, and makes it hard to get this to work
#                in a true concurrent environment.
#
# New sync commands:
#
# Named barriers: these allow us to synchronize N threads, N>2.  Described 
#               below Unnamed barriers.
#
# Unnamed barriers are necessarily for 2 threads.
#               Unnamed barriers have the same syntax as the original
#               sync/sync_thread commands and are a re-implementation 
#               of those commands.   
#
#               Each thread has an implicit unnamed barrier for itself.
#
#               That's what the old command "sync" is waiting on now. 
#               The old command "sync_thread t1" now syncs on that thread's
#               implicit unnamed barrier, and
#               sync_thread t1 t2 t3 does those syncs in order.
#               So it's not very useful for more than 2 threads unless you
#               can guarantee that the threads won't be waiting on 
#               each other.
#
#               Commands: sync, sync_thread
#
# Named barriers: these allow us to synchronize N threads, N>2, such that
#               none proceeds until all have synced to that barrier.
#               Named barriers must be defined before use, and must be
#               undefined to free up their resources.
#
#               Commands:
#
#                define_named_sync <name> N
#                named_sync <name> [comment for debugging]
#                undef_named_sync <name> [silent]
#
#
# When the main tcl thread goes away, it removes all 
# named barriers, so the undef isn't required, but it can be
# used before operations like define_named_sync for idempotence.
#
# scripts/vol.init calls this to define 10 named sync points, 1,2,...,10
# for the associated number of threads.

# define 10 named sync points:
for {set i 1} {$i <= 10} {incr i} {
	undef_named_sync $i silent
	define_named_sync $i $i
}
unset i
