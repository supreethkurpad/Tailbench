/*<std-header orig-src='shore'>

 $Id: tid_t.cpp,v 1.18.2.4 2009/10/30 23:51:11 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma implementation "tid_t.h"
#endif

#include <w_stream.h>
#include "basics.h"
#include "tid_t.h"

const tid_t tid_t::null(0, 0);
const tid_t tid_t::Max(tid_t::hwm, tid_t::hwm);

#ifdef EXPLICIT_TEMPLATE
template class opaque_quantity<max_gtid_len>;
template class opaque_quantity<max_server_handle_len>;

template ostream &operator<<(ostream &, const opaque_quantity<max_gtid_len> &);
template bool operator==(const opaque_quantity<max_gtid_len> &, 
                            const opaque_quantity<max_gtid_len> &);
template ostream &operator<<(ostream &, const opaque_quantity<max_server_handle_len> &);
template bool operator==(const opaque_quantity<max_server_handle_len> &, 
                            const opaque_quantity<max_server_handle_len> &);
#endif

#ifdef VCPP_BUG_3

bool operator==(const opaque_quantity<max_gtid_len> &l, const opaque_quantity <max_gtid_len> &r) 
{
    return ((l._length==r._length) &&
        (memcmp(l._opaque,r._opaque,l._length)==0));
}
bool operator==(const opaque_quantity<max_server_handle_len> &l, const opaque_quantity <max_server_handle_len> &r) 
{
    return ((l._length==r._length) &&
        (memcmp(l._opaque,r._opaque,l._length)==0));
}

ostream & 
operator<<(ostream &o, const opaque_quantity<max_server_handle_len>    &b) 
{
    return b.print(o);
}

ostream & 
operator<<(ostream &o, const opaque_quantity<max_gtid_len>    &b) 
{
    return b.print(o);
}

#endif /* VCPP_BUG_3 */

