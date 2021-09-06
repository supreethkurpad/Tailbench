// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='STHREAD_H'>

 $Id: references.h,v 1.2 2010/05/26 01:19:47 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

/* This file contains doxygen documentation only */

/**\page REFERENCES References
 *
 *\section REFSYNC Synchronization Primitives
 * These are papers are pertinent to the synchronization primitives used
 * in the threads layer of the storage manager.
 *
 * \subsection JPA1 [JPA1]
 * R. Johnson, I. Pandis, A. Ailamaki.  
 * "Critical Sections: Re-emerging Scalability Concerns for Database Storage Engines" 
 * in Proceedings of the 4th. DaMoN, Vancouver, Canada, June 2008
 * (May be found here: http://www.db.cs.cmu.edu/db-site/Pubs/#DBMS:General)
 *
 * \subsection  B1 [B1]
 * H-J Boehm.
 * "Reordering Constraints for Pthread-Style Locks"
 * HP Technical Report HPL-2005-217R1, September 2006
 * (http://www.hpl.hp.com/techreports/2005/HPL-2005-217R1.pdf)
 *
 * \subsection  MCS1 [MCS1]
 * J.M. Mellor-Crummey, M.L. Scott
 * "Algorithms for Scalable Synchronization on Shared-Memory Multiprocessors"
 * in
 * ACM Transactions on Computer Systems, Vol 9, No. 1, February, 1991, pp
 * 20-65
 * (http://www.cs.rice.edu/~johnmc/papers/tocs91.pdf)
 *
 * \subsection  SS1 [SS1]
 * M.L.Scott, W.N. Scherer III
 * "Scalable Queue-Based Spn Locks with Timeout"
 * in PPOPP '01, June 18-20, 2001, Snowbird, Utah, USA
 * (http://www.cs.rochester.edu/u/scott/papers/2001_PPoPP_Timeout.pdf)
 *
 * \subsection  HSS1 [HSS1]
 * B. He, M.L.Scott, W.N. Scherer III
 * "Preemption Adaptivity in Time-Published Queue-Based Spin Locks"
 * in Proceedings of HiPC 2005: 12th International Conference, Goa, India, December 18-21,
 * (http://www.springer.com/computer/swe/book/978-3-540-30936-9 and
 * http://www.cs.rice.edu/~wns1/papers/2005-HiPC-TPlocks.pdf)
 *
 *\section REFSMT Shore-MT 
 * These papers describe the Shore-MT release and related work.
 *
 * \subsection JPHA1 [JPHA1]
 * R. Johnson, I. Pandis, N. Hardavellas, A. Ailamaki.
 * "Shore-MT: A Quest for Scalablility in the Many-Core Era"
 * Carnegie Mellon University Technical Report CMU-CS-08-114,
 * April, 2008 (unpublished)
 *
 * \subsection JPHAF1 [JPHAF1]
 * R. Johnson, I. Pandis, N. Hardavellas, A. Ailamaki, B. Falsaff
 * "Shore-MT: A Scalable Storage Manager for the MultiCore Era"
 * in Proceedings of the 12th EDBT, St. Petersburg, Russia, 2009
 * (http://diaswww.epfl.ch/shore-mt/papers/edbt09johnson.pdf)
 *
 *\section REFDLD Deadlock Detection
 * This paper is the basis for the deadlock detection used by
 * the storage manager's lock manager.
 *
 * \subsection KH1 [KH1]
 * E. Koskinen, M. Herlihy
 * "Dreadlocks: Efficient Deadlock Detection"
 * in SPAA '08, June 14-16, 2008, Munich, Germany
 * (http://www.cl.cam.ac.uk/~ejk39/papers/dreadlocks-spaa08.pdf)
 *
 *\section REFHASH Cuckoo Hashing
 * This paper describes cuckoo hashing, a variation of which
 * is used by the storage manager's buffer manager.
 * \subsection P1 [P1]
 * R Pagh 
 * "Cuckoo Hashing for Undergraduates",
 * Lecture note, IT University of Copenhagen, 2006
 * (http://www.it-c.dk/people/pagh/papers/cuckoo-undergrad.pdf)
 *
 *\section ARIES ARIES Recovery
 *
 * Many papers fall under the topic "ARIES"; this is an early
 * paper that describes the logging and recovery.
 *
 *\subsection MHLPS [MHLPS]
 * C. Mohan, D. Haderle, B. Lindsay, H. Parahesh, P. Schwarz,
 * "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking
 * and Partial Rollbacks Using Write-Ahead Logging"
 * IBM Almaden Reserch Center Technical Report RJ6649, Revised 11/2/90
 *
 *\section REFRTREE R*-Tree Indexes
 * This paper describes R*-Trees, the structure of the storage manager's
 * spatial indexes.
 *
 *\subsection BKSS [BKSS]
 * N. Beckmenn, H.P. Kriegel, R. Schneider, B. Seeger, 
 * "The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles"
 * in Proc. ACM SIGMOD Int. Conf. on Management of Data, 1990, pp. 322-331.
 *
 *\section REFBTREE B+-Tree Indexes
 * This describes the key-value locking and index-management locking,
 * as well as the details of how logging and recovery are handled for
 * B+-Trees in ARIES.
 *\subsection MOH1 [MOH1]
 * C. Mohan
 * "Concurrency Control and Recovery Methods for B+-Tree Indexes: ARIES/KVL and ARIES/IM"
 * IBM Almaden Reserch Center Technical Report RJ9715, March 1, 1994
 *
 *\section REFHUGEPAGE1 Huge Pages, hugetlbfs
 *
 * If you have RHEL kernel documentation installed, see:
 * /usr/share/doc/kernel-doc-<version>/Documentation/vm/hugetlbpage.txt
 *
 *\subsection RHEL1 [RHEL1]
 *   http://linux.web.cern.ch/linux/scientific5/docs/rhel/RHELTuningandOptimizationforOracleV11.pdf (access 5/18/2010) for information about using huge pages with
 *   Linux (this is for Red Hat Linux).
 *
 *\subsection LINSYB1 [LINSYB1]
 * http://www.cyberciti.biz/tips/linux-hugetlbfs-and-mysql-performance.html (access 5/18/2010) for information about using hugetlbfs for SyBase.
 *
 *\subsection LINKER1 [LINKER1]
 * http://lxr.linux.no/source/Documentation/vm/hugetlbpage.txt (access 5/18/2010) for information about Linux kernel support for hugetlbfs.
 */

