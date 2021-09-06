:: SPECjbb run.bat
::
:: Command line usage: run.bat [CDROM]
::
::   run.bat             -> run SPECjbb
::   run.bat CDROM       -> run SPECjbb from CDROM

@echo off

if $%OS%$ == $Windows_NT$ goto winnt

:win
command /e:2048 /c runit %1
goto egress

:winnt
@call runit %1
goto egress

:egress

