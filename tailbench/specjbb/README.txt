  SPECjbb2005 (Java Business Benchmark)
Release 1.04                  06/13/2005

SPECjbb2005 is closely based on SPECjbb2000, 
which was inspired by the TPC-C benchmark 
and loosely follows the TPC-C specification 
(Rev 3.2 from August 27, 1996) for its 
schema, input generation, and operation 
profile. TPC-C is a trademark of the 
Transaction Processing Performance 
Council.

SPECjbb2005 is implemented as a Java 5.0 
application emulating a 3-tier system with 
emphasis on the middle tier. All three 
tiers are implemented within the same JVM 
(though the benchmark may spawn several 
JVM instances). SPECjbb2005 is memory 
resident, performs no I/O to disks, has 
only local network I/O, and has no think 
times.

SPECjbb2005 results are not comparable to 
results for either SPECjbb2000 or TPC-C. 
Comparisons to either would be a SERIOUS 
violation of SPEC's run and reporting 
rules and of TPC's "fair use policy."
Violations are subject to penalties.

You must read and agree to the licenses 
(located in the License subdirectory) 
before you can use or install this 
software.  You must also abide by TPC's 
Fair Use policy rules.  See benchmark 
information and credits for 
acknowledgement of contributors to this 
benchmark.

A Java Virtual Machine compatible with 
J2SE 1.5 or later is required. For further 
information on how to install and run the 
benchmark refer to the User's Guide.

Installation

If you are reading this from the CD-ROM, 
follow the steps below to install the 
benchmark.  If you are reading this from 
a hard disk or web server, then someone 
has already installed the benchmark.

The benchmark is normally installed on a 
server using InstallShield Java(tm) 
Edition.  For reportable results you 
must install the SPECjbb2005 software on 
an adequately configured server.  See 
the run rules (located in the docs 
subdirectory) for more information.  
Only run/reporting rules compliant 
results may be published.

You have several choices to install the 
software

   * Basic Java Installation

     From the command line, change into 
     the top level directory on the 
     CD-ROM and execute the Java jar
     file setup.jar:

         java -jar setup.jar

     This is the preferred method of 
     installation if you have a Java
     Development Kit (JDK or SDK), Java 
     Runtime Environment (JRE), or
     otherwise have a Java Virtual
     Machine which can run applications
     on your server.

   * Windows Installation

     Open My Computer and then the 
     CD-ROM. Double click on the 
     setup.exe icon.

     This is the preferred method of 
     installation on Windows computers 
     if you are not sure whether you 
     have a suitable Java Virtual
     Machine, or where it is located. 
     InstallShield will search your
     machine for a JVM suitable to 
     perform the installation.

   * UNIX Installation

     From the command line, change into
     the top level directory on the
     CD-ROM and execute the shell script
     setup.sh.

     On many systems with graphical file 
     browsers, you may also open the top
     level directory on the CD-ROM and 
     double click on the shell script
     file setup.sh.

     This is the preferred method of
     installation on UNIX computers if
     you are not sure whether you have a
     suitable Java Virtual Machine, or
     where it is located.

   * Mount the CD-ROM directly on the
     server

     The benchmarks may be run directly
     from the CD-ROM without
     installation; however this will
     probably give lower performance
     than could be obtained otherwise.

     First read the software licenses
     and be sure that you agree to their
     terms. 

     This is the fastest and easiest
     method of (non) installation for
     reportable results.

Documentation

The latest documentation and technical 
support answers may be found on the SPEC
web site at 

    http://www.spec.org/jbb2005/ 

Documentation is included with this 
benchmark in the docs subdirectory.

   Last updated 05/24/2005
   Samuel Kounev

  ------------------------------------
