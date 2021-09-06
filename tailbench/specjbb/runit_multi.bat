:: SPECjbb runit.bat
:: 
:: Command line usage: runit.bat [CDROM]
::
::   runit             -> run SPECjbb
::   runit CDROM       -> run SPECjbb from CDROM

@echo off
:: Set a number of JVMs
set JVM=2
:: Set JAVA to Java.exe path.
set JAVA=java
:: if JAVA not set, let's find it.
if $%JAVA%$ == $$ goto findjava

goto foundjava

:findjava
:: Note, this algorithm finds the last occurance of java.exe in path.
echo Attempting to find java...
for %%p in ( %PATH% ) do if exist %%p\java.exe set JAVA=%%p\java
if $%JAVA%$ == $$ goto nojava
echo Found java: %JAVA%

:foundjava
@echo on
%JAVA% -fullversion
@echo off
goto stage1

:nojava
echo No java?  Please make sure that the path to java is set in your environment!
echo Current PATH: %PATH%
goto egress

:stage1
set PROPFILE=SPECjbb.props
set JAVAOPTIONS=-Xms256m -Xmx256m
set JBBJARS=.\jbb.jar;.\check.jar
if "%CLASSPATHPREV%" == $$ set CLASSPATHPREV=%CLASSPATH%
set CLASSPATH=
if $%1$ == $CDROM$ goto findcdrom
goto stage2

:stage2
set CLASSPATH=%JBBJARS%;%CLASSPATHPREV%
echo Using CLASSPATH entries:
for %%c in ( %CLASSPATH% ) do echo %%c
@echo on
start /b %JAVA% %JAVAOPTIONS% spec.jbb.Controller -propfile %PROPFILE% 
@echo off
set I=0
:LOOP
set /a I=%I + 1
@echo on
start /b %JAVA% %JAVAOPTIONS% spec.jbb.JBBmain -propfile %PROPFILE% -id %I% > multi.%I% 
@echo off
IF %I% == %JVM% GOTO END
GOTO LOOP
:END
goto egress

:findcdrom
if not $%CDROM%$ == $$ goto foundcdrom
echo Attempting to find your CDROM drive letter...
set DRIVES=C D E F G H I J K L M N O P Q R S T U V W X Y Z
for %%d in ( %DRIVES% ) do if exist %%d:\jbb.jar set CDROM=%%d:
if $%CDROM%$ == $$ goto nocdrom
echo Aha... I think I've found your CDROM drive letter: %CDROM%

:foundcdrom
if not exist %CDROM%\jbb.jar goto nocdrom
set JBBJARS=%CDROM%\jbb.jar;%CDROM%\check.jar
set PROPFILE=%CDROM%\CDrunWin.prp
%CDROM%
goto stage2

:nocdrom
echo I cannot find your CDROM drive.  Perhaps the SPECjbb CDROM is not loaded in 
echo the drive.  Make sure the SPECjbb CDROM is loaded in your CDROM drive and 
echo check that the environmental variable CDROM is set to your cdrom's drive 
echo letter.  Then, try run.bat again.

:egress
