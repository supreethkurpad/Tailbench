/*<std-header orig-src='shore'>

 $Id: io.cpp,v 1.13.2.5 2010/03/19 22:17:53 nhall Exp $

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
#include <w_strstream.h>
#include <w_base.h>

/* Let the compiler do magic for us */

#define        SHOWBASE        setiosflags(ios::showbase)
#define        NOSHOWBASE        resetiosflags(ios::showbase)

static int testnumber=0;

#include <cerrno>

void test(const char *s, int base, int times, bool /* error_ok */ ) 
{
    cout << "*************************************** TEST#  " 
        << testnumber 
        << " (" << times << " times)"
        <<endl;

    char *        endptr = 0;
    int                t=0;
    cout << " String is " << s 
        <<" Base is " << base
        <<endl;

    cout << "__________________  UNSIGNED  " << testnumber <<endl;
    {
        int                  stringdiff1=0, stringdiff2=0;
        int                  errno1=0, errno2=0;
        w_base_t::uint8_t junk= 1, junk2= 1;

        stringdiff1 = int(endptr-s);

        const char *whatcalled = "strtou8";
        errno = 0;
        junk = junk2 = w_base_t::strtou8(s, &endptr, base);
        errno1 = errno2 =  errno;
        stringdiff1 = stringdiff2 = int(endptr-s);
        stringdiff2 = int(endptr-s);
        cout 
            << "T " << testnumber << " "
            << "Result from " << whatcalled << " :" 
            << hex << junk2  << "/x  "
            << oct << junk2  << "/o  "
            << dec << junk2  << "/d  "
            << endl;
        if(errno1 != errno2) {
            cout << "ERROR: MISMATCHed errno" <<endl;
            cout 
                << "errno1 = " << errno1 
                << " errno2 = " << errno2 
                <<endl;
        } else if(errno2) {
            cout << "errno is " << errno <<endl;
        }
        if(junk != junk2) {
            cout << "ERROR: MISMATCHed values" <<endl;
        }
        if(stringdiff1 != stringdiff2) {
            cout << "ERROR: MISMATCHed endptrs" <<endl;
            cout 
                << "endptr1 @ " << stringdiff1 
                << " endptr2 @ " << stringdiff2 
                <<endl;
        }

        /* ******************** TIME w/o so many proc calls ********/
        if(times > 1) {
            for(t = 0; t < times; t++) {
                w_istrstream        tmp(s);
                typedef ios::fmtflags  fmtflags;
                fmtflags f = tmp.flags();
                f = f & ~(ios::basefield);
                f |= ios::skipws | ios::dec;
                tmp.flags(f);

                w_base_t::uint8_t        u8;
                bool                        rangerr=false;
                w_base_t::_scan_uint8(tmp, u8, false, false, rangerr);
            }
            cout << endl;
        }
    }

    cout << "__________________  SIGNED  " << testnumber <<endl;
    {
        w_base_t::int8_t        junk= -1, junk2 = -1;
        int                          stringdiff1=0, stringdiff2=0;
        int                  errno1=0, errno2=0;


        const char *whatcalled = "strtoi8";
        errno = 0;
        junk = junk2 = w_base_t::strtoi8(s, &endptr, base);
        errno1 = errno2 =  errno;
        stringdiff1 = stringdiff2 = int(endptr-s);
        stringdiff2 = int(endptr-s);
        cout 
            << "T " << testnumber << " "
            << "Result from " << whatcalled << " :" 
            << hex << junk2  << "/x  "
            << oct << junk2  << "/o  "
            << dec << junk2  << "/d  "
            << endl;

        if(errno1 != errno2) {
            cout << "ERROR: MISMATCHed errno" <<endl;
            cout 
                << "errno1 = " << errno1 
                << " errno2 = " << errno2 
                <<endl;
        } else if(errno2) {
            cout << "errno is " << errno <<endl;
        } 
        if(junk != junk2) {
            cout << "ERROR: MISMATCHed values" <<endl;
        }
        if(stringdiff1 != stringdiff2) {
            cout << "ERROR: MISMATCHed endptrs" <<endl;
            cout 
                << "endptr1 @ " << stringdiff1 
                << " endptr2 @ " << stringdiff2 
                <<endl;
        }
    }

    testnumber++;
}

int 
main(int argc, const char *argv[])
{

    /* strange stuff & errors */
    const char *strings_odd[] = {
        // we are not conforming on the first 2 for endptr
        // strtoll is not conforming on value for the first two
                // in the signed cases
        "0Xaaaaffffaaaaffffaaaa",         // err for both (20 f characters)
        "-0Xaaaaffffaaaaffffaaaa",         // err for both (20 f characters)
        " -0xffffffffffffffff",         // err for signed
        "18446744073709551616", // max unsigned #+1, overflow for both
        "18446744073709551615", // max unsigned#, overflow for signed #
        "18446744073709551614", // max unsigned#-1 overflow for signed #
        "9223372036854775808", // max signed number+1, overflow for signed
        "-9223372036854775809", // min signed number-1: overflow for signed
        " 0XYabaDabaDoo",
        "    XYabaDabaDoo",
        " - +8",
        " - 9",
        "abc ",                // err if base not 16
        "abcdefg",        // err if base not 16
        "0x",                // err 
        "-a",                // err 
        "0X123456789012345678901234567890",         // err for both (20 f characters)
        "-0X123456789012345678901234567890",         // err for both (20 f characters)
        "\0"
    };
    /* legit, normal */
    const char *strings_normal[] = {
         "9223372036854775807", // max signed number
         "9223372036854775806", // max signed number-1
        "-9223372036854775808", // min signed number
        "-9223372036854775807", // min signed number+1
        "0x00000000fffffffff",
        "0000000000000000000011111",
        " 0x7fffffffffffffff",        
        " -0x7fffffffffffffff",
        "     +0XabcdeYabaDabaDoo",
        "    \t 12345:w",
        "-0777  ",
        "777",
        "+777",
        "0x180604020",
        "\0"
    };

    int bases[4] = { 0, 8, 10, 16 };

    /* First, test some illegit bases */
    /* expect some MISMATCHes */
    cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << endl;
    cout << "*** Illegit base: expecting MISMATCHed "
        << " errno, value, endptrs" <<endl;
    test("123", 5, 1, true);
    cout << "*** Illegit base: expected MISMATCHed "
        << " errno, value, endptrs" <<endl;
    cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << endl;

    cout << endl
        << "Henceforth, should get NO mismatch messages... " <<endl
        <<endl;

    /*
     * argv[1] serves as #times to repeat the test
     */
    int times = 1;
    if(argc>1){
        times = atoi(argv[1]);
        cout << "Count times=" << times <<endl;
    }

    for(int b=0; b<4; b++) {
        int base = bases[b];
        int i=0;
        for(i=0; ;i++) {
            const char *s = strings_odd[i];
            if(! *s) break;
            // don't bother to time the odd ones
            test(s, base, 1, true);
        }
        for(i=0; ;i++) {
            const char *s = strings_normal[i];
            if(! *s) break;
            test(s, base, times, false);
        }
    }

    return 0;
}
