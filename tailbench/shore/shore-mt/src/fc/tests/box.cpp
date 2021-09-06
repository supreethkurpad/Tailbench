/*<std-header orig-src='shore'>

 $Id: box.cpp,v 1.7.2.4 2009/10/30 23:49:02 nhall Exp $

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

#include <w_stream.h>
#include <cstddef>

#include <nbox.h>

int 
main(int argc, char *argv[])
{
    if(argc < 3) {
        cerr << "Usage: dim <box> <box> <box> ..." << endl;
        cerr << "where <box> is <point>/<point>" <<endl;
        cerr << "where <point> is num,num,num" <<endl;
        exit(1);
    }
    int dim = atoi(argv[1]);

    nbox_t u(dim);

    //     BOX:
    //     point 1: values[0], values[1], ..., values[dim-1]
    //     point 2: values[dim], values[dim+1] ..., values[dim + dim-1]
    
    int*   values = new int[dim * 2];

    switch(dim) {
        case 0:
            cout << "nothing" << endl;
            break;
        case 1:
            cout << "intervals on a line"<<endl;
            break;
        case 2:
            cout << "boxes in a plane"<<endl;
            break;
        case 3:
            cout << "cubes in a solid"<<endl;
        case 4:
            cout << "moving cubes in a <space,time> continuum"<<endl;
            break;
    }


    for(int i=0; i < argc-2; i++) {
        char *point = argv[i+2];
        // get the box represented by the next argument:
        //  x1,y1,z1/x2,y2,z2

        for(int pt = 0; pt < 2; pt++) {
            for(int d = 0; d < dim; d++) {
                // get the d'th dimension of point pt

                char *p= point;
                while(*p && *p != ',' && *p != '/') p++;
                *p = '\0';
                values[(pt*dim)+d] = atoi(point);
                point = p+1;
            }
        }

        nbox_t box(dim, values);
        cout << " box= " << box <<endl;

        box.canonize();
        cout << " canonical box= " << box <<endl;

        u += box;
        cout << " universe= " << u <<endl;
    }

    delete[] values;
    return 0;
}

