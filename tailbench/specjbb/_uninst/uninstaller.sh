#!/bin/sh 
# InstallShield (R)
# (c)1996-2004, InstallShield Software Corporation
# (c)1990-1996, InstallShield Corporation
# All Rights Reserved.

main() 
{
	sh_args="$@"
	VERSION=1.0.0.20028
	DBG=0
	while [ $# -gt 0 ] ;
	do
		case "$1" in
		--)	shift; break;;
		-is:javahome) ISJAVAHOME="$2"; shift;;
		-is:log) DBG=1;LOG="$2"; shift;;

		-is:tempdir) ISTEMP_HOME="$2";  shift;;
		-is:silent) SILENT=true;;
		-is:orig) APP_ORIG_HOME="$2"; shift;;
		-is:nospacecheck) disk_space_check_off=1; shift;;
		-is:freediskblocks) FREE_DISK_BLOCKS="$2";  shift;;
		-is:javaconsole) CONSOLE_ON=true;;
		-cp:p) CP_P="$2";  shift;;
		-cp:a) CP_A="$2";  shift;;
		-is:extract) extractAll; exit;;
		-is:in) inputFile=`echo "$2" | sed -e 's/^\"//;s/\"$//'`; executeExternalInstructions "$inputFile"; exit;;
		-is:help) help; exit;;
		-is:version) echo $VERSION; exit;;
		*)	app_args=`awk 'END{ s=sprintf("%s \"%s\"",A,B); print s;}' A="$app_args" B="$1" </dev/null`;;   
		esac
		shift
	done
	initialize
	findFullPath 
	Instructions "$sh_args"
	handleApplicationExitStatus
	if	[ \( "$sv" -eq 0  -o  "$install_jvm" -eq 0 -o "$resolvedJVMVerified" -eq 0 \) ] ; then 
		cleanupandexit "$applicationExitStatus"
	else
		cleanupandexit	1 nojvm			
	fi
}

programAborted()
{
cat  << HASTOP







          Program Aborted. Cleaning up........







          
HASTOP
}

handleAbort()
{
	DBG=0
	cleanupandexit 1 abort
}

noJVMFound()
{
cat  << noJVMFoundSTOP







          This application requires a Java Run Time Environment (JRE)
          to run. Searching for one on your computer was not successful.
          Please use the command line switch -is:javahome to specify 
          a valid JRE.  For more help use the option -is:help.






noJVMFoundSTOP
}

dsCheckFailed()
{
cat  << dsCheckFailedSTOP







          The directory $ISTEMP 
          does not contain enough space to extract temporary files. 
          Please specify a temporary directory using the -is:tempdir 
          option. Use the -is:help option for more information.





dsCheckFailedSTOP
}

cleanupandexit()
{
	pbl 
	if [ \( "$DBG" -ne 1  -a  -d "$ISTEMP" \) ] ; then
		rm -fr "$ISTEMP"
	fi
	[ -n "$2" ] && {
		cls
		case "$2" in
			abort) programAborted;;
			nojvm) noJVMFound;;
			nospace) dsCheckFailed;;
		esac
	}
	exit $1
}

dbg()
{
	if [ "$DBG" -eq 1 ] ; then 
	  if [ -n "$LOG" ] ; then 
			echo  "$1" >> "$LOG"
		fi
	fi
}

pbl()
{ 
	[ -z "$SILENT" ] && echo	
}

Timer()
{
	sleep 5
	foo=`kill -9 $1 > /dev/null 2>&1`
}

convert()
{
	echo "ibase=16; $1" | bc
}

findFullPath()
{
	if [ -x "./$0" ] ; then 
		INSTALLER_PATH="`pwd`/$0"
		elif [ -x "$0" ] ; then
		INSTALLER_PATH="$0"
	fi 
	dbg "INSTALLER_PATH="$INSTALLER_PATH""
}

banner()
{
[ -z "$SILENT" ] && {
cat  << BSTOP







          $pbmesg........
BSTOP
}
}

pbclr()
{
	[ -z "$SILENT" ] && {
		awk ' END {
			printf("%c",13);
			printf("%s","          ");
			i=length(pbmesg);
			for (k=1 ; k <= i; k++ ) printf("%c",32);
		}' pbmesg="$pbmesg"  </dev/null 
	}
}

pb()
{
	[ -z "$SILENT" ] && {
		awk ' END {
			printf("%c",13);
			printf("%s","          ");
			printf("%s",pbmesg);
			for (k=1 ; k <= i; k++ )
				printf("%c",46);
		}' pbmesg="$pbmesg" i=$pbc </dev/null 
		pbc=`expr $pbc % 8`
	}
}

cls()
{
	[ -z "$SILENT" ] && clear
}

isNumeric() 
{
	if  [ -n "$1" ] ; then 
		num=`echo "$1" | sed 's/[0-9]*//g' 2>/dev/null`
		case ${num} in  
		"") echo 0;;     
		 *) echo 1;; 
		esac
	else
		echo 1	
	fi 
}

extractAll()
{
	findFullPath
	[ -z "$ISTEMP_HOME" ] && ISTEMP_HOME=`pwd`
	SILENT=true
	initialize
	OFFSET=0
	FIXED_BLOCK_SIZE=1024
	if [ ! -x "$INSTALLER_PATH" ] ; then 
		dbg "Can't locate the installer archive. Extraction failed."
		cleanupandexit 100
	fi 
  I=0
  while [ $I -lt $FILEINDEXCOUNT ] ; do
    II=`awk 'END{ s=sprintf("$FILEINDEX%s", I); print s }' I=$I </dev/null 2>/dev/null`
    eval II=$II
    dbg "$II"
		SIZE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[3] }' II=$II </dev/null  2>/dev/null`
		OFFSET=`awk  'END{ split(II,FIELDS, ":"); print FIELDS[4] }' II=$II </dev/null 2>/dev/null`
		NAME=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[5]  }' II=$II </dev/null 2>/dev/null`
		SIZE=`convert $SIZE`
	  OFFSET=`convert $OFFSET`
	  NAME="$ISTEMP/$NAME"
	  dbg "NAME=$NAME SIZE=$SIZE OFFSET=$OFFSET"
		checkDiskSpace "$SIZE" "$ISTEMP" "$NAME"
		[ $disk_space_check -ne 0 ] && {
			DBG=0
			cleanupandexit 1 nospace
		}
	  extractAFile
		I=`expr $I + 1`  
  done
  echo "$I files extracted in the directory $ISTEMP"
}

extractAFile()
{
	[ $ismpVV ] && dbg "extracting the file $NAME of SIZE=$SIZE at OFFSET=$OFFSET"
	if [ "$SIZE" -le "$FIXED_BLOCK_SIZE" ] ; then 
		BOFFSET=`expr $OFFSET /  "$FIXED_BLOCK_SIZE"`	  
		dd if="$INSTALLER_PATH" of="$ISTEMP/filePadded" bs="$FIXED_BLOCK_SIZE"  skip="$BOFFSET"  count=1 > /dev/null 2>&1
		dd if="$ISTEMP/filePadded" of="$NAME" bs="$SIZE" count=1 > /dev/null 2>&1
		rm -f "$ISTEMP/filePadded"
	else 
		BOFFSET=`expr $OFFSET /  "$FIXED_BLOCK_SIZE"`	
		BLOCKS=`expr $SIZE /  "$FIXED_BLOCK_SIZE"`
		BLOCKS=`expr $BLOCKS + 1`
		dd if="$INSTALLER_PATH" of="$ISTEMP/filePadded" bs="$FIXED_BLOCK_SIZE"  skip="$BOFFSET"  count=$BLOCKS > /dev/null 2>&1
		dd if="$ISTEMP/filePadded" of="$NAME" bs="$SIZE" count=1 > /dev/null 2>&1
		rm -f "$ISTEMP/filePadded"
	fi 
}

extractJVMFiles()
{
	if [ -f "$ISTEMP/jvmfit" ] ; then 
		c=`wc -l "$ISTEMP/jvmfit" | awk '{ print $1 }'`
		I=1
		while [ $I -le $c ] ; do
			II=`sed -n -e "${I}p" $ISTEMP/jvmfit`
			SIZE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[3] }' II=$II </dev/null  2>/dev/null`
			OFFSET=`awk  'END{ split(II,FIELDS, ":"); print FIELDS[4] }' II=$II </dev/null 2>/dev/null`
			NAME=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[5]  }' II=$II </dev/null 2>/dev/null`
			SIZE=`convert $SIZE`
			OFFSET=`convert $OFFSET`
			NAME="$ISTEMP/$NAME"
			extractAFile
			if [ -f "$NAME" ] ; then 
				sed "s///;s/^[ ]*//;s/[ ]*$//" "$NAME" >> "$NAME.sed" 2>/dev/null
				rm -f "$NAME" 
				mv $NAME.sed $NAME
				echo "$NAME"  >> "$ISTEMP/jvmlist"
			fi 
			I=`expr $I + 1`  	
	 done
	 fi
	 [ $ismpVV ] && { 
		 dbg "reading jvm list..." ; 
		 if [ -f "$ISTEMP/jvmlist" ] ; then 
		 	cat 	"$ISTEMP/jvmlist" >> $LOG 2>/dev/null; 
		 fi
	 }
}

extractVerifyJar()
{
I=0
while [ $I -lt $FILEINDEXCOUNT ] ; do
	II=`awk 'END{ s=sprintf("$FILEINDEX%s", I); print s }' I=$I </dev/null 2>/dev/null`
	eval II=$II
	TYPE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[1];  }' II=$II </dev/null 2>/dev/null`
	
	TYPE=`convert $TYPE`
	if [ "$TYPE" -eq "$VERIFY_CLASS_TYPE" ] ; then 
		SIZE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[3] }' II=$II </dev/null  2>/dev/null`
		OFFSET=`awk  'END{ split(II,FIELDS, ":"); print FIELDS[4] }' II=$II </dev/null 2>/dev/null`
		NAME=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[5]  }' II=$II </dev/null 2>/dev/null`
		SIZE=`convert $SIZE`
		OFFSET=`convert $OFFSET`
		NAME="$ISTEMP/$NAME"
		extractAFile
		break
	fi 
	I=`expr $I + 1`  
done
}

makeJVMFit()
{
I=0
while [ $I -lt $FILEINDEXCOUNT ] ; do
	II=`awk 'END{ s=sprintf("$FILEINDEX%s", I); print s }' I=$I </dev/null 2>/dev/null`
	eval II=$II
	TYPE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[1];  }' II=$II </dev/null 2>/dev/null`
	TYPE=`convert $TYPE`
	if [ "$TYPE" -eq "$JVM_FILE_TYPE" ] ; then 
	 echo $II >> $ISTEMP/jvmfit
	fi
	I=`expr $I + 1`  	
done
}

initialize()
{
	set +o notify
	trap handleAbort  2  
	FIXED_BLOCK_SIZE=1024
	DISK_BLOCK_SIZE=512
	POSIXLY_CORRECT=1; export POSIXLY_CORRECT
	pbc=1
	# the following file TYPE[S] are defined by ISMP builder. Any change to the  TYPE number should confirm to the type number defined here.
	VERIFY_CLASS_TYPE=5
	APP_ARCHIVE_TYPE=6
	JVM_FILE_TYPE=1
	EMBED_JARS_TYPE=3
	BIN_FILE_TYPE=7
	JVM_INSTALLER_TYPE=4
	sv=2  #sv -  jvm search&verify result; successful search&verify sets to zero; initializing to non-zero 
	resolve=2 #resolution of matched jvm file result that is required for launching the java app. successful resolution sets to zero; initializing to non-zero 
	verify=2 # verification of a JVM result. successful verification of searched JVM sets to zero; initializing to non-zero 
	install_jvm=2 # result of bundled JVM installation. successful installation sets to zero;  initializing to non-zero
	resolvedJVMVerified=2 #app launcher verifies the embedded resolved JVM file and set the value to 0, if verification is successful. installer launcher does NOT use this. initializing to non-zero.
	IS_JVM_TEMP=0 #set to 1 upon the successful installation of bundled JVM; initializing to zero.
	uimode=2 # the default ui mode of the app is gui which corresponds uimode to non-zero. a console mode sets to  zero.
	[ -z "$disk_space_check_off" ] && disk_space_check_off=0 #  disk_space_check_off is not defined, switching it on by default. 
	if [ -z "$ISTEMP_HOME" -a -d "/tmp" -a -w "/tmp" ] ; then
	ISTEMP_HOME="/tmp"
	fi
	[ -z "$ISTEMP_HOME" ] && ISTEMP_HOME="$HOME"
	if [ ! -d "$ISTEMP_HOME" ] ; then 
 		mkdir -p "$ISTEMP_HOME" >/dev/null 2>&1
 		if [ $? -ne 0 ] ; then 
			cls
			tempLocationErrMesg "$ISTEMP_HOME"
			cleanupandexit 1
 		fi
	fi
	rand=$$`date '+%j%H%M%S'` 
	rand=`echo "$rand" | sed 's/[a-zA-Z]//g;s/%//g'`
	if [ `isNumeric $rand` -ne 0 ] ; then 
		rand=$$
	fi
	
	ISTEMP="$ISTEMP_HOME/istemp$rand"
	if [ ! -d "$ISTEMP" ] ; then
		mkdir $ISTEMP >/dev/null 2>&1
		if [ $? -ne 0 ] ; then 
			cls
			tempLocationErrMesg "$ISTEMP"
			cleanupandexit 1
		fi
	fi 
	
	awk 'END{ print "Test file created by MultiPlatform installer" >> a ;}' a="$ISTEMP/writecheck" </dev/null 2>/dev/null
	if [ -f "$ISTEMP/writecheck" ] ; then 
		rm -f "$ISTEMP/writecheck"
	else
			cls
			tempLocationErrMesg "$ISTEMP"
			cleanupandexit 1
	fi 
	[ -n "$ismpVV" ] && dbg "Temp directory used by shell script launcher = $ISTEMP"
	if [ -n "$LOG" ] ; then 
		[ ! -d `dirname $LOG` ] &&		LOG=`pwd`/`basename "$LOG"` 
		awk 'END{ s=sprintf("Log file created by MultiPlatform installer @%s",b); print s  >> a ;}' a="$LOG" b="`date`"</dev/null 2>/dev/null
		if [ ! -f "$LOG" ] ; then 
			LOG=/dev/null
		fi
	else
		 LOG=/dev/null
	fi 
	[ `awk 'END{ if(index(a,"-silent") > 0) {print 0; } else { print 2;} }' a="$app_args"	</dev/null 2>/dev/null` -eq 0 ] &&  SILENT=true
	[ `awk 'END{ if(index(a,"-console") > 0) {print 0; } else { print 2;} }' a="$app_args"	</dev/null 2>/dev/null` -eq 0 ] &&  uimode=0
	[ -z "$SILENT" ] && setpbmesg
	[ -z "$pbmesg" ] && pbmesg="Initializing"
	cls
	banner
}

writetab()
{
	awk '	BEGIN {	processingTag=0 }
					$0 == "/:" { if (beginprocessingTag == 1) beginprocessingTag=0 ; next }
					$0 == tag { beginprocessingTag=1; next }
					{ if (beginprocessingTag == 1) { print $0 >> tab; next } }
					END { } ' tab="$2" tag="$3" "$1"			
}

searchAndVerify()
{
	pbclr
	pbmesg="Searching JVM"
	dbg "--------------------------------------------------------------------------------------------"
	dbg "Searching a JVM using $1"
	if [ -f "$ISTEMP/pathHint" ] ; then 
		rm -f "$ISTEMP/pathHint"
	fi 

	JVM_EXE=`awk ' BEGIN { FS=":" } /^JVM_EXE/ { print $2; exit }' "$1" 2>/dev/null`
	JVM_EXE=`echo "$JVM_EXE" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//;s/\"//g'`
	if [ -z "$JVM_EXE" ] ; then 
		[ $ismpVV ] && dbg  "value of JVM_EXE is an empty string in JVM FILE="$JVM_FILE". Trying next JVM File."
		return
	else 
		[ $ismpVV ] && dbg "value of JVM_EXE=$JVM_EXE in JVM FILE=$JVM_FILE"
	fi
	
	writetab "$1"  "$ISTEMP/pathHint" "PATH_HINT:"
	if [ -f "$ISTEMP/pathHint" ] ; then 
		[ $ismpVV ] && { dbg "using path hints in the JVM file $1";  }
		pathHintc=`wc -l "$ISTEMP/pathHint" | awk '{ print $1 }'`
		pathHintcc=1
		while [ $pathHintcc -le $pathHintc ] ; do
			pbc=`expr $pbc + 1`
			pb
			PathHint=`sed -n -e "${pathHintcc}p;s/^[ ]*//;s/[ ]*$//" "$ISTEMP/pathHint"`
                        pathSep=`echo $PathHint | grep "^/" | wc -l`
                        IS_RELATIVE=0
                        if [ $pathSep -eq 0 ] ; then
				if [ -z "$APP_ORIG_HOME" ] ; then 
					PathHint="`dirname $INSTALLER_PATH`/$PathHint"
				else
					PathHint="$APP_ORIG_HOME/$PathHint"
				fi
                                IS_RELATIVE=1
                        fi
			if [ -f "$1.fr" ] ; then 
				rm -f "$1.fr"
			fi
			[ $ismpVV ] && dbg "using the pathHint=$PathHint."
			if [ -d "$PathHint" ] ; then 
				for x in $PathHint ; 
				do 
					if [ -x "$x/$JVM_EXE" ] ; then 
						echo "$x/$JVM_EXE" >> "$1.fr.shellxpansion"
					fi
					if [ -x "$x/jre/$JVM_EXE" ] ; then 
						echo "$x/jre/$JVM_EXE" >> "$1.fr.shellxpansion"
					fi
				done
					[ $ismpVV ]  && { dbg "reading find result from shell expansion..." ; cat "$1.fr.shellxpansion" >> $LOG 2>/dev/null; }
			fi

			find  $PathHint/$JVM_EXE  > "$1.fr.findcommand" 2>/dev/null
			if [ $? -eq 0 ] ; then 
				if [ -f  "$1.fr.findcommand" ] ; then 
				 [ $ismpVV ] && { dbg "reading find result from find command..." ; cat "$1.fr.findcommand" >> $LOG 2>/dev/null; }	
					frc=`wc -l "$1.fr.findcommand" | awk '{ print $1 }'` 
					frcc=1
					while [ $frcc -le $frc ] ; do
						frl=`sed -n -e "${frcc}p" "$1.fr.findcommand"`
						grep "$frl" "$1.fr.shellxpansion" 1>/dev/null 2>&1
						if [ $? -ne 0 ] ; then 
						 echo "$frl" >> "$1.fr.shellxpansion"
						fi 
						frcc=`expr $frcc + 1` 	
					done
				fi 
			fi

			if [ -f  "$1.fr.findcommand" ] ; then 
				rm -f "$1.fr.findcommand"
			fi

			if [ -f  "$1.fr.shellxpansion" ] ; then 
				mv  "$1.fr.shellxpansion" "$1.fr"
				rm -f "$1.fr.shellxpansion"
			fi

			if [ -f "$1.fr" ] ; then 
				[ $ismpVV ] && { dbg "reading find result after merging..." ; cat "$1.fr" >> $LOG 2>/dev/null; }
				frc=`wc -l "$1.fr" | awk '{ print $1 }'` 
				frcc=1
				while [ $frcc -le $frc ] ; do
					frl=`sed -n -e "${frcc}p" "$1.fr"`
					jvm_exe=`echo $JVM_EXE | sed 's/\//\\\\\//g'`
					VerifyJVM "$1" "$frl"
					if [ $verify -eq 0 ] ; then 
						J=`echo "$frl"  | sed "s/${jvm_exe}//"`
						J=`echo "$J" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
						echo "JVM_HOME:$J" >> "$1"
						RESOLVED_JVM="$1"
						if [ $IS_RELATIVE -eq 1 ] ; then
							IS_JVM_TEMP=1
							DESTINATION_DIR=$MEDIA_DIR
						fi
						sv=0
						dbg "Verification passed for $frl  using the JVM file $1." 
						rm -f "$1.fr"
						return
					else
						 dbg "Verification failed for $frl using the JVM file $1."
					fi 
					frcc=`expr $frcc + 1` 	
				done
			else
				 [ $ismpVV ] &&  { dbg "find result is empty for the pathhint=$PathHint"; }
			fi	
			pathHintcc=`expr $pathHintcc  + 1`
		done
	else
		dbg "path hint is not specified in the JVM file $1"
	fi
}

VerifyJVM()
{
	 pbclr
	 pbmesg="Verifying JVM"
 	 pb
 	 [ ! -f "$ISTEMP/Verify.jar" ] && extractVerifyJar
	 awk '	BEGIN {	begin=0; i=1 }
					$0 == "/:" {	if (begin == 1) begin=0 ;	next; }
					$0 == tag {	 begin=1; 	next;	}
					{ if (begin== 1) { item[i]=$0; 	i++; 	next;	}	}
					END { for (k=1; k < i; k++)  print item[k] >> tab; } '  tab="$ISTEMP/sp" tag="JVM_PROPERTIES:"  "$1" 2>/dev/null
	 
	 if [ -f "$ISTEMP/sp" ] ; then 
		spc=`wc -l "$ISTEMP/sp" | awk '{ print $1 }'` 
		spcc=1
		systemprops=
		while [ $spcc -le $spc ] ; do 
			spl=`sed -n -e "${spcc}p"  "$ISTEMP/sp"`
			spl=`echo "$spl" | sed 's/\"//g'`
			systemprops=`awk 'END { i=index(spl,"="); s=substr(spl,1,i-1); ss=sprintf("%s %s", sp, s); print ss; } ' spl="$spl" sp="$systemprops" </dev/null 2>/dev/null`
			spcc=`expr $spcc + 1`
		done
		
		jvm_classpath=
	  cp_switch=`awk 'BEGIN{ FS=":"}  $1 == tag { print $2; exit; }' tag=CLASSPATH $1`
	  cp_switch=`echo "$cp_switch" | sed 's/\"//g'`
	  jvm_classpath=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_CLASSPATH $1`
	  if [ -z "$jvm_classpath" ] ; then 
			dbg "Verifying... $2 $cp_switch $ISTEMP/Verify.jar Verify $systemprops"
			eval "$2" $cp_switch "$ISTEMP/Verify.jar" Verify $systemprops 1>"$ISTEMP/jvmout" 2>/dev/null&
			bgpid=$!
			Timer $bgpid&
			wait $bgpid 1>/dev/null 2>&1
		else
			jb=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_EXE $1 2>/dev/null`
			jb=`echo "$jb" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
			jb=`echo "$jb" | sed 's/\//\\\\\//g'`
			JVM_HOME=`echo "$2"  | sed "s/${jb}//"`
			eval jvm_classpath="$jvm_classpath"
			dbg "Verifying... $2 $cp_switch $jvm_classpath:$ISTEMP/Verify.jar Verify $systemprops"
			eval "$2" $cp_switch "$jvm_classpath":"$ISTEMP/Verify.jar" Verify $systemprops 1>"$ISTEMP/jvmout" 2>/dev/null&
			bgpid=$!
			Timer $bgpid&
			wait $bgpid 1>/dev/null 2>&1
			JVM_HOME=
		fi 
			
		if [ -f "$ISTEMP/jvmout" ] ; then 
			spc=`wc -l "$ISTEMP/sp" | awk '{ print $1 }'` 
			spcc=1
			systemprops=
			while [ $spcc -le $spc ] ; do 
				spl=`sed -n -e "${spcc}p"  "$ISTEMP/sp"`
				spl=`echo $spl | sed 's/\"//g'`
				jvmfilevalue=`awk 'END { i=index(spl,"="); s=substr(spl,i+1); print s } ' spl="$spl" sp="$systemprops" </dev/null 2>/dev/null`
				jvmoutc=`expr $spcc + 1`
				jvmout=`sed -n -e "${jvmoutc}p"  "$ISTEMP/jvmout"`
				verify=`awk ' END {
					exactMatch=1
					verify=2
					len = length(jvmfilevalue)
					for (k = len ; k >len-3 ; k--) {
						char=substr(jvmfilevalue, k, 1);
						s = sprintf("%s%s", s,char);
					}
					if (length(s) == length("...")) {
						if ( index(s, "...") == 1) {
							exactMatch=0
						}
					}
					if (exactMatch == 1) {
						if ( (length(jvmfilevalue) == length(jvmout)) && (index(jvmfilevalue, jvmout) == 1) ) 	verify=0
					} else  {
						jvmfilevalue_prefix=substr(jvmfilevalue, 1, len-3)
						if (index(jvmout,jvmfilevalue_prefix) == 1 ) verify=0
					}
					if (length(ismpVV) > 0) {
						printf("jvm system property specified in jvm file=%s\n",jvmfilevalue) >> ilog
						printf("jvm system property from running Verify diagnostics on the JVM=%s\n",jvmout) >> ilog
						if (verify == 0) {
							if (exactMatch == 1) {
								print "exact match of system property succeeded" >> ilog	
							} else {
								print "non-exact match of system property succeeded" >> ilog	
							}
						} else {
							if (exactMatch == 1) {
								print "exact match of system property failed" >> ilog	
							}
							else {
								print "non-exact match of system property failed" >> ilog	
							}
						}
					}
					print verify
				} ' jvmout="$jvmout" jvmfilevalue="$jvmfilevalue" ismpVV="$ismpVV" ilog="$LOG" </dev/null 2>/dev/null`
				if [ $verify -ne 0 ] ; then 
					break
				fi 
				spcc=`expr $spcc + 1`
			done
		else 
			dbg "$ISTEMP/jvmout does not exist. JVM Verification process may have failed."
		fi 
	else
		dbg "system properties are not specified in "$1""
	fi
	rm -f "$ISTEMP/sp"
	rm -f "$ISTEMP/jvmout"
}

searchParitions()
{
		for i in $1 ; do 
			find ${i} -type d \
			\( -name '*[jJ][dD][kK]*'  \
			-o -name '*[jJ][rR][eE]*'  \
			-o -name '*[jJ][aA][vV][aA]*' \
			-o -name '*[jJ]2[rR][eE]*' \
			-o -name '*[jJ]2[sS][eE]*' \
			\)  >>  "$ISTEMP/searchParitionsResult" 2>/dev/null
		done
}

writesptab()
{
	 sed 's/^[ ]*//;s/[ ]*$//' "$1" >> "$1.tmp" 2>/dev/null
	 mv "$1.tmp" "$1"
	 awk '	BEGIN {	begin=0; i=1}
					$0 == "/:" {	if (begin == 1) begin=0 ;	next; }
					$0 == tag {	 begin=1; 	next;	}
					{ if (begin== 1) { item[i]=$0; 	i++; 	next;	}	}
					END { for (k=1; k < i; k++) {  s=sprintf("%s%s;",s,item[k]); } s=sprintf("%s%s",s,JF); print s >> tab } ' JF="$1" tab="$ISTEMP/sp" tag="JVM_PROPERTIES:"  "$1" 2>/dev/null
}

verifyJavaHome()
{
	if [ -f "$ISTEMP/jvmlist" ] ; then 
		jvmlc=`wc -l "$ISTEMP/jvmlist" | awk '{ print $1 }'`
		cc=1
		while [ $cc -le $jvmlc ] ; do
			JVM_FILE=`sed -n -e "${cc}p" "$ISTEMP/jvmlist" 2>/dev/null`
			if [ -f "$JVM_FILE" ] ; then 
				JVM_EXE=`awk ' BEGIN { FS=":" } /^JVM_EXE/ { print $2; exit }' "$JVM_FILE"`
				JVM_EXE=`echo "$JVM_EXE" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//;s/\"//g'`
				if [ -z "$JVM_EXE" ] ; then 
					[ $ismpVV ] && dbg  "value of JVM_EXE is an empty string in JVM FILE="$JVM_FILE". Trying next JVM File"
					continue 
				else 
					[ $ismpVV ] && dbg "value of JVM_EXE=$JVM_EXE in JVM FILE="$JVM_FILE""
				fi
				if [ -x "$ISJAVAHOME/$JVM_EXE" ] ; then 
					[ $ismpVV ] && dbg "Verifying $ISJAVAHOME using the JVM file $JVM_FILE"
					VerifyJVM "$JVM_FILE" "$ISJAVAHOME/$JVM_EXE"
					if [ $verify -eq 0 ] ; then 
						J="$ISJAVAHOME"
						J=`echo "$J" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
						echo "JVM_HOME:$J" >> "$JVM_FILE"
						RESOLVED_JVM="$JVM_FILE"
						sv=0
						dbg "Verification passed for $ISJAVAHOME using the JVM file $JVM_FILE." 
						return
					else 
						dbg "Verification failed for $ISJAVAHOME  using the JVM file $JVM_FILE"
					fi 
				fi
			fi 
			cc=`expr $cc + 1`  	
		done
	else
		[ $ismpVV ] && dbg "jvm files not specified. Verification cannot be performed for JVM specfied using $ISJAVAHOME"
	fi
}

checkEnvironment() 
{
	dbg "Checking the environment variables specifed in the JVM files to find the JVM..." 
	if [ -f "$ISTEMP/jvmlist" ] ; then 
		checkEnvlc=`wc -l "$ISTEMP/jvmlist" | awk '{ print $1 }'`
		checkEnvcc=1
		while [ $checkEnvcc -le $checkEnvlc ] ; do
			checkEnvJvmFile=`sed -n -e "${checkEnvcc}p" "$ISTEMP/jvmlist" 2>/dev/null`

			for pathVariable in `echo $PATH|sed 's/:/ /g'`
				do
			  		if [ -x "$pathVariable/java" ] ; then
		        	 		VerifyJVM "$checkEnvJvmFile" "$pathVariable/java"
						if [ $verify -eq 0 ] ; then 
							pathVariable=`echo $pathVariable | sed 's/\/bin//g'`
							echo "JVM_HOME:$pathVariable" >> "$checkEnvJvmFile"
							RESOLVED_JVM="$checkEnvJvmFile"
							sv=0
							dbg "Verification passed for $pathVariable/$checkEnvJvmExe using the JVM file $checkEnvJvmFile." 
							return
						fi 
			  		fi
			 	done
			checkEnvJvmExe=`awk ' BEGIN { FS=":" } /^JVM_EXE/ { print $2; exit }' "$checkEnvJvmFile" 2>/dev/null`
			checkEnvJvmExe=`echo "$checkEnvJvmExe" | sed 's/^[ ]*//;s/[ ]*$//'`
			if [ -z "$checkEnvJvmExe" ] ; then 
				[ $ismpVV ] && dbg  "value of JVM_EXE is an empty string in JVM FILE="$checkEnvJvmFile". Trying next JVM File."
				checkEnvcc=`expr $checkEnvcc + 1`	
				continue
			else 
				[ $ismpVV ] && dbg "value of JVM_EXE=$JVM_EXE in JVM FILE=$checkEnvJvmFile"
			fi
			if [ -f "$ISTEMP/platformHint" ] ; then 
				rm -f "$ISTEMP/platformHint"
			fi 
			writetab "$checkEnvJvmFile"  "$ISTEMP/platformHint" "PLATFORM_HINT:"
			if [  -f  "$ISTEMP/platformHint" ] ; then 
				[ $ismpVV ] && dbg "using platform hints or environment variables in the JVM file $checkEnvJvmFile"
				platformHintlc=`wc -l "$ISTEMP/platformHint" | awk '{ print $1 }'`
				platformHintcc=1
				while [ $platformHintcc -le $platformHintlc ] ; do
					platformHintLine=`sed -n -e "${platformHintcc}p;s/^[ ]*//;s/[ ]*$//" "$ISTEMP/platformHint" | sed 's/\(.*\)/$\1/'`
					eval platformHintEvaled=$platformHintLine
					if [ -n "$platformHintEvaled" ] ; then 
						if [ -x "$platformHintEvaled/$checkEnvJvmExe" ] ; then 
							dbg "A jvm found at $platformHintEvaled/$checkEnvJvmExe" 
							VerifyJVM "$checkEnvJvmFile"  "$platformHintEvaled/$checkEnvJvmExe"
							if [ $verify -eq 0 ] ; then 
								platformHintEvaled=`echo "$platformHintEvaled" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
								echo "JVM_HOME:$platformHintEvaled" >> "$checkEnvJvmFile"
								RESOLVED_JVM="$checkEnvJvmFile"
								sv=0
								dbg "Verification passed for $platformHintEvaled/$checkEnvJvmExe using the JVM file $checkEnvJvmFile." 
								return
							else
								dbg "Verification failed for $platformHintEvaled/$checkEnvJvmExe using the JVM file $checkEnvJvmFile." 
							fi 
						fi
					else 
							dbg "$platformHintLine is not defined in the shell environment specifed in the JVM FILE `basename $checkEnvJvmFile`"
					fi 
					platformHintcc=`expr $platformHintcc + 1` 	
				done
			else
				dbg "platform hints or environment variables are not specified in the JVM file $checkEnvJvmFile"
			fi 
			checkEnvcc=`expr $checkEnvcc + 1`				
		done
	else
		[ $ismpVV ] && dbg "jvm files not specified in the launcher. Environment Variables can not be checked."
	fi
}

searchJVM()
{
		makeJVMFit
		extractJVMFiles

		[ "$ISJAVAHOME" ] && { 
			dbg "command line switch -is:javahome is specified. Verifying the JVM with the JVM files specifed with the launcher."
			verifyJavaHome
			if [ $sv -eq 0 ] ; then 
				return
			else
				dbg "JVM specified with -is:javahome cannot be verified with the JVM files specified with the launcher. Environment Variables will be checked next..."	
			fi
		}

		
		checkEnvironment 
		if [ $sv -eq 0 ] ; then 
			return
	  else
	  	dbg "No JVM can  be found using the shell environment variable. Searching JVM will continue with Path Hints specified in the JVM Files..."	
		fi 

		JVM_FILE=
		if [ -f "$ISTEMP/jvmlist" ] ; then 
			jvmlc=`wc -l "$ISTEMP/jvmlist" | awk '{ print $1 }'`
			cc=1
			while [ $cc -le $jvmlc ] ; do
				JVM_FILE=`sed -n -e "${cc}p" "$ISTEMP/jvmlist" 2>/dev/null`
				if [ -f "$JVM_FILE" ] ; then 
						searchAndVerify "$JVM_FILE"
					if [ $sv -eq 0 ] ; then 
							dbg "jvm found and verification passed for $JVM_FILE." 
							break
					fi
				fi 
				cc=`expr $cc + 1`  	
			done
		else                                                                                                                                                                                                                                                            
			dbg "jvm files not specified. Searching a JVM can not be performed."
		fi
}

aggregatecp()
{
	aggregatecpc=`wc -l "$1" | awk '{ print $1 }'`
	aggregatecpcc=1
	while [ $aggregatecpcc -le $aggregatecpc ] ; do
		aggregatecpl=`sed -n -e "${aggregatecpcc}p" "$1" 2>/dev/null`
		aggregatecpl=`echo "$aggregatecpl" | sed 's/\"//g' 2>/dev/null`
		AGGREGATECP=`awk ' END { s=sprintf("\"%s\"",b); s=sprintf("%s:%s",a,s); printf s }' a="$AGGREGATECP" b="$aggregatecpl" </dev/null 2>/dev/null`
		aggregatecpcc=`expr $aggregatecpcc + 1`
	done
	echo "$AGGREGATECP"
}

extractArchiveJar()
{
	[ -n "$ismpVV" ] && dbg "Installer JAR archive is embedded. Extracting ..."				
	I=0
	while [ $I -lt $FILEINDEXCOUNT ] ; do
		II=`awk 'END{ s=sprintf("$FILEINDEX%s", I); print s }' I=$I </dev/null 2>/dev/null`
		eval II=$II
		TYPE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[1];  }' II=$II </dev/null 2>/dev/null`
		TYPE=`convert $TYPE`
		if [ "$TYPE" -eq "$APP_ARCHIVE_TYPE" ] ; then 
			SIZE=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[3] }' II=$II </dev/null  2>/dev/null`
			OFFSET=`awk  'END{ split(II,FIELDS, ":"); print FIELDS[4] }' II=$II </dev/null 2>/dev/null`
			NAME=`awk 'END{ split(II,FIELDS, ":"); print FIELDS[5]  }' II=$II </dev/null 2>/dev/null`
			SIZE=`convert $SIZE`
			OFFSET=`convert $OFFSET`
			NAME="$ISTEMP/$NAME"
			checkDiskSpace "$SIZE" "$ISTEMP" "$NAME"
			[ $disk_space_check -ne 0 ] && cleanupandexit 1 nospace
			pbclr
			pbmesg="Extracting Installation Archive"
			pb
			extractAFile
			INSTALLER_ARCHIVE="$NAME"
			break
		fi 
		I=`expr $I + 1`  
	done
}

extractEmbeddedJar()
{
	embeddedJarc=0
	while [ $embeddedJarc -lt $FILEINDEXCOUNT ] ; do
		FI_KEY=`awk 'END{ s=sprintf("$FILEINDEX%s", I); print s }' I=$embeddedJarc </dev/null 2>/dev/null`
		eval FI_VALUE=$FI_KEY
		TYPE=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[1];  }' a=$FI_VALUE </dev/null 2>/dev/null`

		TYPE=`convert $TYPE`
		if [ "$TYPE" -eq $EMBED_JARS_TYPE ] ; then 
			SIZE=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[3] }' a=$FI_VALUE </dev/null  2>/dev/null`
			OFFSET=`awk  'END{ split(a,FIELDS, ":"); print FIELDS[4] }' a=$FI_VALUE </dev/null 2>/dev/null`
			NAME=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[5]  }' a=$FI_VALUE </dev/null 2>/dev/null`
			SIZE=`convert $SIZE`
			OFFSET=`convert $OFFSET`
			NAME="$ISTEMP/$NAME"
			dbg "extracting embedded jars in the archive."
			checkDiskSpace "$SIZE" "$ISTEMP" "$NAME"
			[ $disk_space_check -ne 0 ] && cleanupandexit 1 nospace 
			extractAFile
			EMBEDDED_JARS=`awk ' END { s=sprintf("\"%s\"",b); s=sprintf("%s:%s",a,s); printf s }' a="$EMBEDDED_JARS" b="$NAME" </dev/null 2>/dev/null`
		fi 
		embeddedJarc=`expr $embeddedJarc + 1`  
	done
}

parseJavaArgs()
{
		awk '$0 !~ /^-/ && /.*,.*/{ 
			if (length($0) > 1 ) {
					t=split($0,FIELDS,",")
					if (t == 2) print FIELDS[2];
			}
		} 
		/^-/{ printf("%s\n",$0); }' |  sed 's/\\//g;s/)//;s/^[ ]*//;s/[ ]*$//;s/^\"//;s/\"$//;s/@\(.*\)@\(.*\)/\1=\2/'    >> "$ISTEMP/javaargs"
}

aggregateJavaArgs()
{
	#<jvmfile> <jvmargsfile-oneperline>
	javaArgsc=`wc -l "$2" | awk '{ print $1 }'`
	javaArgscc=1
	while [ $javaArgscc -le $javaArgsc ] ; do 
		javaArgsl=`sed -n -e "${javaArgscc}p" "$2"`
		javaArgsl=`echo "$javaArgsl" | sed 's/^[ ]*//;s/[ ]*$//'` 
		if [ `awk 'END {  print index(a,"-")  }' a="$javaArgsl" </dev/null 2>/dev/null` -ne 1 ] ; then 
			javaArgsResolved=`awk '{ split(A, FIELDS,"=");
																					if ( index($0, FIELDS[1]) == 1 )  {
																						i=index($0, ":") ;
																						if ( i > 1 ) { 
																							s=substr($0, i+1); 
																							if (length(FIELDS[2]) > 0) {
																								s=sprintf("%s%s",s, FIELDS[2]); 
																							} 
																							print s; 
																							exit;
																						}
																				}
																	}' A="$javaArgsl"  "$1"` 
			if [ -n "$javaArgsResolved" ] ; then 
				[ -n "$ismpVV" ] && dbg "Java Argument `echo "$javaArgsl" | cut -d= -f1` is resolved to $javaArgsResolved in the JVM FILE $1"
			else
				dbg "Java argument `echo "$javaArgsl" | cut -d= -f1` is not defined in the JVM file $1."
			fi
		else 
			javaArgsResolved="$javaArgsl"
			[ -n "$ismpVV" ] && dbg "using literal Java Argument $javaArgsResolved"
		fi 
		
		if [ -n "$javaArgsResolved" ] ; then 
			JAVA_ARGS=`awk 'END{ printf("%s %s", a, b); }' a="$javaArgsResolved" b="$JAVA_ARGS" </dev/null 2>/dev/null`
		fi
		javaArgscc=`expr $javaArgscc + 1`
	done
}

resolveRuntimeJavaArgs()
{
	[ -n "$ismpVV" ] && dbg "resolving runtime java args"
	jaFile=`modifyFileName "$INSTALLER_PATH" ja`
	if [ -f "$jaFile" ] ; then  
		cat "$jaFile" | sed 's/%IF_EXISTS%//' | parseJavaArgs
	else
		dbg "Run time Java arguments are not specified."
	fi
}

resolveBuildTimeJavaArgs()
{
	[ -n "$ismpVV" ] && dbg "resolving buildtime java args"
	if [ -n "$JAVA_ARGS_UNRESOLVED" ] ; then 
		echo "$JAVA_ARGS_UNRESOLVED" | sed 's/%IF_EXISTS%//g;s/) */+/g;s/\(-[^ .]*\)/\1+/g' | tr '+' '\012' | sed 's/^[ ]*//;s/[ ]*$//' |   parseJavaArgs
		[ \( -f "$ISTEMP/javaargs" -a  -n "$ismpVV" \)  ] && cp "$ISTEMP/javaargs" "$ISTEMP/javaargs.buildtime"
	else
		dbg "Build time Java arguments are not specified."
	fi
}

modifyFileName()
{
	installerDir=`dirname "$1"`
	installerDir=`echo "$installerDir" | sed 's/\//\\\\\//g'` 
	awk 	'END {
		t=split(a,FIELDS,".");
		if ( t > 1 ) {
			for (i=1; i<=t-1;i++) s=sprintf("%s.%s",s,FIELDS[i]);
		}
		else {
		 s=a;
		}
		print s;
	}' a=`basename "$1"` </dev/null 2>/dev/null 	| sed "s/^\.//;s/\.$//;s/\(.*\)/${installerDir}\/\1.${2}/"	
}

processCommandLineCP()
{
	if [ `awk 'END{ if (index(a,"@") == 1)  print 0 ;  else print 1; }' a="$1" </dev/null 2>/dev/null` -eq 0 ] ; then 
		 file=`awk 'END{ s=substr(a, 2); print s; }' a="$1" </dev/null 2>/dev/null`
		 [ -f "$file" ] &&	CommandLineCP=`aggregatecp "$file"`
	else 
		CommandLineCP=`awk 'END{ 
			t=split(a,FIELDS,":");
			for ( i=1; i<=t; i++) {
			 if ( length(FIELDS[i]) > 0 ) {
				 quotedCPItem=sprintf("\"%s\"",FIELDS[i]); 
				 s=sprintf("%s:%s",s,quotedCPItem); 
			 }
			}
			printf s;
		}' a="$1" </dev/null 2>/dev/null`
	fi
	echo "$CommandLineCP" | sed 's/^://'
}

resolveLaunchCommand()
{
		if [ -f "$1" ] ; then 
				JVM_HOME=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_HOME "$1" 2>/dev/null`
				JVM_HOME=`echo "$JVM_HOME" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
				[ ! -d "$JVM_HOME" ] &&   {
				 	dbg "JavaHome is not resolved correctly in the jvm file $1. Failed to launch the application."
				 	return
				}
				JVM_EXE=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_EXE "$1" 2>/dev/null`
				JVM_EXE=`echo "$JVM_EXE" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//;s/\"//g'`
				[ -z "$JVM_EXE" ] &&   {
				 	dbg "Javaexe is not resolved correctly or empty in the jvm file $1. Failed to launch the application."
				 	return
				}
				CLASSPATH_SWITCH=`awk 'BEGIN{ FS=":"}  $1 == tag { print $2; exit; }' tag=CLASSPATH "$1" 2>/dev/null`
				CLASSPATH_SWITCH=`echo "$CLASSPATH_SWITCH" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//;s/\"//g'`
				[ -z $CLASSPATH_SWITCH ] && { 
					dbg "Classpath switch is not specified in the jvm file $1. Failed to launch the application."
					return
				} 
				
				if [ -n "$CP_P" ] ; then 
					PREPEND_CLASSPATH=`processCommandLineCP  "$CP_P"`
				else
				 	dbg "-cp:p operand is empty" 	
				fi 
				if [ -n "$CP_A" ] ; then 
					APPEND_CLASSPATH=`processCommandLineCP  "$CP_A"`
				else
					dbg "-cp:a operand is empty" 
				fi
				cp1File=`modifyFileName "$INSTALLER_PATH" cp1`
				[ -f "$cp1File" ] && { 
					CP1=`aggregatecp "$cp1File"`
					dbg "classpath specified in the $cp1File=$CP1"
				}
				JVM_CLASSPATH=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_CLASSPATH "$1" 2>/dev/null`
				
				cpFile=`modifyFileName "$INSTALLER_PATH" cp`
				[ -f "$cpFile" ] && { 
					CP=`aggregatecp "$cpFile"`
					dbg "classpath specified in the $cpFile=$CP"
				}

				if [ -z "$APPLICATION_ARCHIVE" ] ; then 
					dbg "Installer JAR archive is not embedded."
					APPLICATION_ARCHIVE=
				elif [ "$APPLICATION_ARCHIVE" = EMBED ] ; then 
					extractArchiveJar
					if [ -f "$INSTALLER_ARCHIVE" ] ; then 
						archiveSize=`wc -c  "$INSTALLER_ARCHIVE" | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }' 2>/dev/null`
						if [ $archiveSize -ne $SIZE ] ; then 
							archiveSize=`ls -l  "$INSTALLER_ARCHIVE" | awk '{ if ( $5 ~ /^[0-9]/ ) print $5 ; else print 0; }' 2>/dev/null`
							if [ $archiveSize -ne $SIZE ] ; then 
								dbg "Extracted Installer JAR archive file size incorrect. archive may be corrupt. Failed to launch the application."
								return
							else
								APPLICATION_ARCHIVE="$INSTALLER_ARCHIVE"	
							fi
						else
							APPLICATION_ARCHIVE="$INSTALLER_ARCHIVE"
						fi
					else
						dbg "Error extracting Installer JAR archive from shell script wrapper.  Failed to launch the application."
						return
					fi
				elif [ -n "$APP_ORIG_HOME"  ] ; then 
					if [ -f "$APP_ORIG_HOME"/"$APPLICATION_ARCHIVE" ] ; then 
						dbg "Installer JAR archive is not embedded. Copying from $APP_ORIG_HOME/$APPLICATION_ARCHIVE"
						rclSize=`wc -c  "$APP_ORIG_HOME/$APPLICATION_ARCHIVE" | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }' 2>/dev/null`
						checkDiskSpace "$rclSize" "$ISTEMP" "$APP_ORIG_HOME/$APPLICATION_ARCHIVE"
						[ $disk_space_check -ne 0 ] && cleanupandexit 1 nospace
						cp "$APP_ORIG_HOME/$APPLICATION_ARCHIVE" "$ISTEMP/$APPLICATION_ARCHIVE"
						if [ $? -eq 0 ] ; then 
							INSTALLER_ARCHIVE="$ISTEMP/$APPLICATION_ARCHIVE"
						else 
							dbg "Error copying the Installer JAR archive from the CD media to temp location on the host machine. Failed to launch the application."
							return
						fi
					else 
						dbg "Installer JAR archive is not located in the CD media location. Failed to launch the application."
						return
					fi 
				else
					dbg "CD media location is not defined and Installer JAR archive can not be located. Failed to launch the application."
					return
				fi

				[ -n "$INSTALLER_ARCHIVE" ] && INSTALLER_ARCHIVE=`awk 'END { s=sprintf("\"%s\"",a); print s; }' a="$INSTALLER_ARCHIVE" </dev/null 2>/dev/null`
				
				extractEmbeddedJar
				
				cp2File=`modifyFileName "$INSTALLER_PATH" cp2`
				[ -f "$cp2File" ] && { 
					CP2=`aggregatecp "$cp2File"`
					dbg "classpath specified in the $cp2File=$CP2"
				}
				
				spFile=`modifyFileName "$INSTALLER_PATH" sp`
				[ -f "$spFile" ] && { 
						runtimespc=`wc -l "$spFile" | awk '{ print $1 }'`
						runtimespcc=1
						while [ $runtimespcc -le $runtimespc ] ; 
						do
							runtimespl=`sed -n -e "${runtimespcc}p" "$spFile" 2>/dev/null`
							RUNTIME_SYSTEMPROP=`awk 'END{ s=sprintf("%s -D%s ",a,b); print s; }' a="$RUNTIME_SYSTEMPROP" b="$runtimespl" </dev/null 2>/dev/null`
							runtimespcc=`expr $runtimespcc + 1`
						done
					dbg "java runtime system properties specified in $spFile file = $RUNTIME_SYSTEMPROP"
				}
				resolveBuildTimeJavaArgs	"$1"	#<jvmfile>	
				resolveRuntimeJavaArgs "$1"	#<jvmfile>	
				if [ -f "$ISTEMP/javaargs" ] ; then 
					awk 'L[$0]++ == 0' "$ISTEMP/javaargs" >>  "$ISTEMP/javaargs.nodups" ; rm -f "$ISTEMP/javaargs" 
					aggregateJavaArgs  "$1"  "$ISTEMP/javaargs.nodups"  #<jvmfile> <jvmargsfile-oneperline>
					[ "$DBG" -ne 1 ] && rm -f "$ISTEMP/javaargs.nodups"
				else 
					dbg "Warning: internal error parsing Java arguments. Launcher command may be missing Java Arguments."
				fi
				
				IS_JVM_FILE="$1"
				IS_JVM_HOME="$JVM_HOME"
				IS_LAUNCHER_FILE="$INSTALLER_PATH"
				IS_TEMP_DIR="$ISTEMP_HOME"
				if [ -n "$APP_ORIG_HOME" ] ; then 
					MEDIA_DIR="$APP_ORIG_HOME"
				else
					MEDIA_DIR=`dirname "$INSTALLER_PATH"`
				fi 
				[ -z "$LOG" ] && LOG=/dev/null
				APP_STDERR=">$ISTEMP/APP_STDERR" # make sure APP_STDERR is always assigned.  
				[ \( -z "$SILENT" -a -n "$CONSOLE_ON" \) ] && APP_STDERR="&1" # do re-direction if necessary.
				if [ "$uimode" -eq  0 ] ; then  
					APP_STDOUT="&1"
				else
					APP_STDOUT=">$LOG" # make sure APP_STDOUT is always assigned.  
					[ \( -z "$SILENT" -a -n "$CONSOLE_ON" \) ] && APP_STDOUT="&1" # do re-direction if necessary.
				fi
				resolve=0
				cd `dirname $INSTALLER_PATH`	
	else 
		dbg "resolved jvm file can not be found. Failed to launch the application."
	fi 
}

installBundledJRE()
{
	[ $ismpVV ] && dbg "installing bundled JRE..."
	installJVMc=0
	while [ $installJVMc -lt $FILEINDEXCOUNT ] ; do
		FI_KEY=`awk 'END{ s=sprintf("$FILEINDEX%s", I); print s }' I=$installJVMc </dev/null 2>/dev/null`
		eval FI_VALUE=$FI_KEY
		TYPE=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[1];  }' a=$FI_VALUE </dev/null 2>/dev/null`

		TYPE=`convert $TYPE`
		if [ "$TYPE" -eq $JVM_INSTALLER_TYPE ] ; then 
			SIZE=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[3] }' a=$FI_VALUE </dev/null  2>/dev/null`
			OFFSET=`awk  'END{ split(a,FIELDS, ":"); print FIELDS[4] }' a=$FI_VALUE </dev/null 2>/dev/null`
			NAME=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[5]  }' a=$FI_VALUE </dev/null 2>/dev/null`
			SIZE=`convert $SIZE`
			OFFSET=`convert $OFFSET`
			NAME="$ISTEMP/$NAME"
			dbg "Extracting bundled JRE..."
			pbclr
			pbmesg="Extracting Bundled JRE"
			pb
			checkDiskSpace "$SIZE" "$ISTEMP" "$NAME"
			[ $disk_space_check -ne 0 ] && cleanupandexit 1 nospace
			extractAFile
			break
		fi 
		installJVMc=`expr $installJVMc + 1`  
	done

		if [ ! -f "$NAME" ] ; then 
			dbg "$NAME is not found. Error extracting bundled JRE. Failed to launch the application."
			return
		else 
			[ $ismpVV ] && dbg "extracting bundled JRE successful."
		fi
		whereAmI=`pwd`
		mkdir "$ISTEMP/_bundledJRE_" >/dev/null 2>&1
		checkDiskSpace "$NAME" "$ISTEMP"
		[ $disk_space_check -ne 0 ] && cleanupandexit 1 nospace
		cd "$ISTEMP/_bundledJRE_"
		dbg "installing bundled JRE..."
		pbclr
		pbmesg="Installing Bundled JRE"
		pb
		"$NAME" -qq  >/dev/null 2>&1
		if [ $? -ne 0 ] ; then 
			cd "$whereAmI"		
			dbg "Error installing bundled JRE. Failed to launch the application."
			rm -f "$NAME"
			return
		else
			cd "$whereAmI"
			[ $ismpVV ] && dbg "installing bundled JRE successful."
			rm -f "$NAME"
		fi 
		chmod -R 755 "$ISTEMP/_bundledJRE_" > /dev/null 2>&1
		if [ -f "$ISTEMP/_bundledJRE_/jvm" ] ; then 
					sed "s///;s/^[ ]*//;s/[ ]*$//" "$ISTEMP/_bundledJRE_/jvm" >> "$ISTEMP/_bundledJRE_/jvm.sed" 2>/dev/null
					mv "$ISTEMP/_bundledJRE_/jvm.sed"  "$ISTEMP/_bundledJRE_/jvm"
			else
				dbg "Invalid bundled JRE. jvm file is missing. Installation of bundled JRE is not successful."
				return
			fi
		JVM_EXE=`awk 'BEGIN { FS=":" } /^JVM_EXE/ { print $2; exit }' "$ISTEMP/_bundledJRE_/jvm" 2>/dev/null`
		if [ -z "$JVM_EXE" ] ; then 
			dbg  "value of JVM_EXE is an empty string in JVM FILE=$ISTEMP/_bundledJRE_/jvm. Bundled JRE jvm file is incorrect. Installation of bundled JRE is not successful."
			return
		else 
			[ $ismpVV ] && dbg "value of JVM_EXE=$JVM_EXE in JVM FILE=$ISTEMP/_bundledJRE_/jvm"
		fi		
		JVM_EXE=`echo "$JVM_EXE" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//;s/\"//g'`
		if [ -x "$ISTEMP/_bundledJRE_/$JVM_EXE" ] ; then 
			[ $ismpVV ] && dbg "Verifying $ISTEMP/_bundledJRE_/$JVM_EXE using the JVM file $ISTEMP/_bundledJRE_/jvm"
			VerifyJVM "$ISTEMP/_bundledJRE_/jvm" "$ISTEMP/_bundledJRE_/$JVM_EXE"
			if [ $verify -eq 0 ] ; then 
				J="$ISTEMP/_bundledJRE_"
				J=`echo "$J" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
				echo "JVM_HOME:$J" >> "$ISTEMP/_bundledJRE_/jvm"
				RESOLVED_JVM="$ISTEMP/_bundledJRE_/jvm"
				IS_JVM_TEMP=1
				install_jvm=0
				dbg "Verification passed for $ISTEMP/_bundledJRE_ using the JVM file $ISTEMP/_bundledJRE_/jvm." 
				return
			else 
				dbg "Verification failed for $ISTEMP/_bundledJRE_ using the JVM file $ISTEMP/_bundledJRE_/jvm"
			fi 
		else 
			dbg "$ISTEMP/_bundledJRE_/$JVM_EXE does not exist. Bundled JRE is not a working JVM. Installation of bundled JRE is not successful."
			return
		fi
}

checkDiskSpace()
{
	[ "$disk_space_check_off" -eq 1 ]  && {
		disk_space_check=0
		dbg "disk space checking turned off/skipped". 
		return
	}
	disk_space_check=2 # disk space check result; successful check sets it to 0; initializing to non-zero value
	cdsBlocksRequired=reset  #will be assigned to a numeric value; initializing to a non-numeric value
	cdsAvailable=reset	#will be assigned to a numeric value; initializing to a non-numeric value
	mkdir "$ISTEMP/emptydir"
	# Linux reports the default block-size in 1024 bytes units. Forcing it to report in 512 bytes units.
	du --block-size=$DISK_BLOCK_SIZE "$ISTEMP" 2>/dev/null 1>&2
	if [ $? -eq 0 ] ; then 
		cdsDefaultBlocksAllocated=`du --block-size=$DISK_BLOCK_SIZE "$ISTEMP/emptydir" | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }' 2>/dev/null`
	else
		cdsDefaultBlocksAllocated=`du "$ISTEMP/emptydir" | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }' 2>/dev/null`
	fi
	[ $ismpVV ] &&	dbg "default blocks allocated by the filesystem=$cdsDefaultBlocksAllocated"
	rm -fr "$ISTEMP/emptydir"
	if [ -f "$1" ] ; then 
		dbg "checking disk space on the parition $2 for $1"
		[ ! -x "$1" ] && chmod 744 "$1" >/dev/null 2>&1
		"$1" -t >/dev/null 2>&1
		[ $? -ne 0 ] && {
			dbg "Bundled JRE is not binary compatible with host OS/Arch or it is corrupt.  Testing bundled JRE failed."
			return
		}
		cdsUnZipFileSize=`"$1"  -c | wc -c  | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }' 2>/dev/null`
		[ $ismpVV ] &&	dbg "Unziped file size = $cdsUnZipFileSize"
		[ "$cdsUnZipFileSize" -eq 0 ] && { 
			dbg "Error calculating the uncompressed size of bundled JRE or it is corrupt. Installation of bundled JRE failed."
			return
		}
		cdsFilesCount=`"$1"  -t | wc -l  | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }' 2>/dev/null`
		[ $ismpVV ] &&	dbg "Unziped file count = $cdsFilesCount"
		[ "$cdsFilesCount" -eq 0 ] && { 
			dbg "Error calculating the file count  of bundled JRE or it is corrupt. Installation of bundled JRE failed."
			return
		}
		cdsBlocksRequired=`echo \("$cdsUnZipFileSize" \/ \("$DISK_BLOCK_SIZE" - \("$DISK_BLOCK_SIZE" \* 1\/100\)\)\) + \("$cdsFilesCount" \* "$cdsDefaultBlocksAllocated" \) | bc`
	elif [ `isNumeric $1` -eq 0 ] ; then 
		 [ -n "$3" ] &&  dbg "checking disk space on the parition $2 for $3"
		cdsBlocksRequired=`echo "$1" \/ \($DISK_BLOCK_SIZE - \($DISK_BLOCK_SIZE \* 1/100\)\) + "$cdsDefaultBlocksAllocated" | bc`
	else
		dbg "Internal Disk space check error. Size of file to be copied or extracted to the host file system is reported incorrectly."
		return
	fi
	
	if [ `isNumeric $cdsBlocksRequired` -ne 0 ]  ; then 
		dbg "Error calculating required file blocks. "
		[ $ismpVV ] &&	dbg "required 512 blocks for $3 = $cdsBlocksRequired"
		return
	else
		dbg "$cdsBlocksRequired 512 bytes disk blocks required."
	fi
	
	[ -z "$FREE_BLOCKS_COLUMN" ] && FREE_BLOCKS_COLUMN=4
	if [ -n "$FREE_DISK_BLOCKS" ] ; then 
		cdsAvailable="$FREE_DISK_BLOCKS"
	else
		# gnu df by default reports the free blocks in 1K size, forcing it to report to 512 sizes
		df -P --block-size="$DISK_BLOCK_SIZE" "$2" 2>/dev/null 1>&2 
		if [ $? -eq 0 ] ; then 
			# Linux df is POSIX complaint
			cdsAvailable=`df  -P  --block-size="$DISK_BLOCK_SIZE" "$2" | awk ' NR > 1 { print $A }' A="$FREE_BLOCKS_COLUMN" 2>/dev/null`
		else 
			df -P "$2" 2>/dev/null 1>&2
			# If POSIX complaint
			if [ $? -eq 0 ] ; then 
				cdsAvailable=`df  -P  "$2" | awk ' NR > 1 { print $A }' A="$FREE_BLOCKS_COLUMN" 2>/dev/null`
			# Is Solaris xpg4 available
			elif  [ -x /usr/xpg4/bin/df ] ; then 
				cdsAvailable=`/usr/xpg4/bin/df -P "$2"  | awk ' NR > 1 { print $A }' A="$FREE_BLOCKS_COLUMN" 2>/dev/null`
			else
			# if it comes this far, guess  the column 4 of df as free-available blocks. This may be incorrect. 
				cdsAvailable=`df  "$2" | awk '{ print $A }' A="$FREE_BLOCKS_COLUMN" 2>/dev/null`
			fi
		fi 		
	fi
	if [ `isNumeric $cdsAvailable` -ne 0 ] ; then 
		dbg "Disk space check error. free disk [ of 512 bytes ] blocks can not be found or incorrectly reported by df command on disk parition containing the directory $2."
		dbg "Please specify free disk [ of 512 bytes ] blocks on the disk partition containing the directory $2 by setting the environment variable FREE_DISK_BLOCKS"
		dbg "and run the installer again."
		[ $ismpVV ] &&	dbg "available 512 blocks in the partition containing the directory $2 = $cdsAvailable"
		return
	else
		dbg "$cdsAvailable $DISK_BLOCK_SIZE bytes disk blocks  available on the partition $2"
	fi 

	if [ $cdsAvailable -gt $cdsBlocksRequired ] ; then 
		dbg "Disk space check on the parition $2 succeeded."
		disk_space_check=0
	else
		if [ `expandDiskPartition $2 $cdsBlocksRequired $cdsAvailable` -ne 0 ] ; then 
		  dbg "$2 partition expanded successfully."
			disk_space_check=0			
		else 
		dbg "Insufficient disk space in the disk parition containing the directory $2. $cdsBlocksRequired 512 bytes file blocks required. $cdsAvailable 512 bytes file blocks only is available."
		dbg "Free up the parition or specify a different parition to write temporary files using the command line switch -is:tempdir <tempdir>."
		dbg "Temporary files will be cleanedup after the installation." 
	fi
	fi
}

executeLauncher()
{
	[ $ismpVV ] && dbg "running inner launcher".
	[ $ismpVV ] && dbg "original command line args $@"
	[ "$FILEINDEXCOUNT" -ne 1 ]  && { 
		dbg "Error extracting inner launcher. Failed to launch the application."
		return
	}
	TYPE=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[1];  }' a=$FILEINDEX0 </dev/null 2>/dev/null`
	TYPE=`convert $TYPE`
	if [ "$TYPE" -eq $BIN_FILE_TYPE ] ; then 
		SIZE=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[3] }' a=$FILEINDEX0 </dev/null  2>/dev/null`
		OFFSET=`awk  'END{ split(a,FIELDS, ":"); print FIELDS[4] }' a=$FILEINDEX0 </dev/null 2>/dev/null`
		NAME=`awk 'END{ split(a,FIELDS, ":"); print FIELDS[5]  }' a=$FILEINDEX0 </dev/null 2>/dev/null`
		SIZE=`convert $SIZE`
		OFFSET=`convert $OFFSET`
		NAME="$ISTEMP/$NAME.embedded"
		extractAFile
	fi 
	INNER_LAUNCHER="$NAME"
	if [ -f "$INNER_LAUNCHER" ] ; then 
		archiveSize=`wc -c  "$INNER_LAUNCHER" | awk '{ if ( $1 ~ /^[0-9]/ ) print $1 ; else print 0; }'`
		if [ $archiveSize -ne $SIZE ] ; then 
			archiveSize=`ls -l  "$INNER_LAUNCHER" | awk '{ if ( $5 ~ /^[0-9]/ ) print $5 ; else print 0; }'`
			if [ $archiveSize -ne $SIZE ] ; then 
				dbg "Error extracting inner launcher. Archive may be corrupt. Failed to launch the application."
				return
			fi
		fi
	else
		dbg "Inner launcher can not be found.  Failed to launch the application."
		return
	fi
	MEDIA_LOCATION=`dirname $INSTALLER_PATH`
	runInnerLauncher=`awk 'END{ s=sprintf("%s -is:orig %s %s", c, b, a); print s; }' a="$@" b="$MEDIA_LOCATION" c="$INNER_LAUNCHER" </dev/null 2>/dev/null`
	chmod 744 "$INNER_LAUNCHER"
	dbg "running inner launcher=$runInnerLauncher"
	eval "exec $runInnerLauncher"
	exit 0
}

tempLocationErrMesg()
{
cat << END
          Temporary directory, $1 
          does not exist and can not be created. The current user may not 
          have the necessary permission to write to the directory. Please 
          specify another temporary directory with the option -is:tempdir. 
          Use the -is:help option for more information. 
END
}

help()
{
cat  << STOP
Usage : `basename $0` [OPTIONS] [APPLICATION_ARGUMENTS]
  
Where OPTIONS include:
     
   -is:javahome <dir>  JRE home which points to directory containing bin/java.
   -is:tempdir <dir>   Temporary directory to be used by the launcher.
   -cp:a <classpath>   Append <classpath> to the launcher's classpath.
   -cp:p <classpath>   Prepend <classpath> to the launcher's classpath.
   -is:log <filename>  Log debug messages to <file>.
   -is:extract         Extracts the contents of the archive.
   -is:nospacecheck    Turns off the launcher disk space checking. 
   -is:version         Returns launcher version and exits.
   -is:help            Prints this help message and exits
STOP
}

VerifyResolvedJVM()
{
	dbg "Verifying resolved JVM"
	makeJVMFit
	extractJVMFiles
	verifedJVMFile=`cat "$ISTEMP/jvmlist" |  xargs awk '/^JVM_HOME:.+/{ print FILENAME; exit }' 2>/dev/null`
	#Verify the resolved JVM file 	
	if [ -f "$verifedJVMFile" ] ; then
		JVM_HOME=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_HOME "$verifedJVMFile" 2>/dev/null`
		JVM_HOME=`echo "$JVM_HOME" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
		[ ! -d "$JVM_HOME" ] &&   {
			dbg "JVM_HOME is not found in the jvm file $verifedJVMFile."
			return
		}
		JVM_EXE=`awk 'BEGIN { FS=":" }  $1 == tag { i=index($0,":"); s=substr($0,i+1); print s; exit; }' tag=JVM_EXE "$verifedJVMFile" 2>/dev/null`
		JVM_EXE=`echo "$JVM_EXE" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//;s/\"//g'`
		[ -z "$JVM_EXE" ] &&   {
			dbg "Javaexe is not found or empty in the jvm file $verifedJVMFile."
			return
		}
		VerifyJVM 	"$verifedJVMFile" "$JVM_HOME/$JVM_EXE"
		if [ $verify -eq 0 ] ; then 
			RESOLVED_JVM="$verifedJVMFile"
			resolvedJVMVerified=0
			dbg "Verification passed for $JVM_HOME/$JVM_EXE using the JVM file $verifedJVMFile." 
		else
			dbg "Verification failed for $JVM_HOME/$JVM_EXE using the JVM file $verifedJVMFile." 
		fi 
	else
		dbg "resolved jvm file can not be found.	Try to  launch the application by searching a JVM..."
	fi
}

executeExternalInstructions()
{
	if [ -f "$1" ] ; then 
		SILENT=true
		initialize
		LOG=`awk '/^LOG/ { i=index($0,"="); s=substr($0,i+1); print s; exit }' "$1" 2>/dev/null`
		[ -n "$LOG" ] && { 
			touch "$ISTEMP/$LOG"
			if [ -f "$ISTEMP/$LOG" ] ; then 
				LOG="$ISTEMP/$LOG"
				DBG=1
			else
				LOG=
			fi
		}
		
		action=`awk '/^ACTION/ { i=index($0,"="); s=substr($0,i+1); print s; exit }' "$1" 2>/dev/null`
		if [ -n "$action" ] ; then 
			if [ "$action" = "SEARCHJVM" ] ; then 
				dbg "searching JVM"
				
				awk '/^JVMFILE[0-9]+/ { i=index($0,"="); s=substr($0,i+1); print s >> f; }' f="$ISTEMP/jvmlist" "$1" 2>/dev/null
				if [ -f "$ISTEMP/jvmlist" ] ; then 
					jvmFileslc=`wc -l  "$ISTEMP/jvmlist" | awk '{ print $1 }' 2>/dev/null`
					if [ "$jvmFileslc" -gt  0 ] ; then 
						VERIFY_JAR=`awk '/^VERIFY_JAR/ { i=index($0,"="); s=substr($0,i+1); print s ; }'  "$1" 2>/dev/null`
						if [ -f "$VERIFY_JAR" ] ; then
							cp "$VERIFY_JAR" "$ISTEMP/."
							if [ -f "$ISTEMP/Verify.jar" ] ; then 
								jvmFilescc=1
								while [ "$jvmFilescc" -le "$jvmFileslc" ] ; do
									JVM_FILE=`sed -n -e "${jvmFilescc}p" "$ISTEMP/jvmlist" 2>/dev/null`
									if [ -f "$JVM_FILE" ] ; then 
												sed "s///;s/^[ ]*//;s/[ ]*$//" "$JVM_FILE" >> "$JVM_FILE.sed" 2>/dev/null
												rm -f "$JVM_FILE" 
												mv "$JVM_FILE.sed"  "$JVM_FILE"
									fi
									jvmFilescc=`expr "$jvmFilescc" + 1`
								done

								checkEnvironment
								
								if [ $sv -eq 0 ] ; then 
									dbg "jvm found using an environment variable and verfication passed for $JVM_FILE." 
									JVM_HOME=`awk '/^JVM_HOME/ { i=index($0,":"); s=substr($0,i+1); print s ; }' "$RESOLVED_JVM" 2>/dev/null`
									echo 1
									echo "$JVM_HOME"
									echo "$RESOLVED_JVM"
								else
									jvmFilescc=1
									while [ "$jvmFilescc" -le "$jvmFileslc" ] ; do
										JVM_FILE=`sed -n -e "${jvmFilescc}p" "$ISTEMP/jvmlist" 2>/dev/null`
										if [ -f "$JVM_FILE" ] ; then 
											searchAndVerify "$JVM_FILE"
											if [ "$sv" -eq 0 ] ; then 
													dbg "jvm found and verfication passed for $JVM_FILE." 
													JVM_HOME=`awk '/^JVM_HOME/ { i=index($0,":"); s=substr($0,i+1); print s ; }' "$RESOLVED_JVM" 2>/dev/null`
													echo 1
													echo "$JVM_HOME"
													echo "$RESOLVED_JVM"
													break
											fi
										fi 
										jvmFilescc=`expr "$jvmFilescc" + 1`
									done
								fi
							else 
								dbg "Error copying Verify.jar . JVM Verification can't be performed."
							fi
						else
							dbg "Verify class is missing . JVM Verification can't be performed."
						fi 
					else
						dbg "JVM files are not specified. JVM search can't be performed."
					fi
				else
					dbg "JVM file list is missing. JVM search can't be performed."
				fi
				
			elif  [ "$action" = "INSTALL_JVM" ] ; then 
				dbg "Installing JVM"
				BUNDLED_JRE=`awk '/^BUNDLED_JRE/ { i=index($0,"="); s=substr($0,i+1); print s; exit }' "$1" 2>/dev/null`
				if [ -f "$BUNDLED_JRE" ] ; then 
				 	dbg "Bundled JRE file is = $BUNDLED_JRE"
				 	DESTINATION_DIR=`awk '/^DESTINATION_DIR/ { i=index($0,"="); s=substr($0,i+1); print s; exit }' "$1" 2>/dev/null`
				 	if [ -n "$DESTINATION_DIR" ] ; then 
				 		dbg "Destination directory is = $DESTINATION_DIR"
				 		touch "$DESTINATION_DIR/iswritable" 2>/dev/null 1>&2
				 		if [ \( $? -eq 0 \) -a \( -f "$DESTINATION_DIR/iswritable" \) ] ; then 
				 		
				 			dbg "Beginning to install bundled JRE."
				 			rm -f "$DESTINATION_DIR/iswritable"
							whereAmI=`pwd`
							mkdir "$DESTINATION_DIR" >/dev/null 2>&1
							checkDiskSpace "$BUNDLED_JRE" "$DESTINATION_DIR"
							if [ "$disk_space_check" -eq 0 ] ; then 
								cd "$DESTINATION_DIR"
								dbg "installing bundled JRE..."
								"$BUNDLED_JRE" -qq  >/dev/null 2>&1
								if [ $? -ne 0 ] ; then 
									cd "$whereAmI"		
									dbg "Error installing bundled JRE. Failed to launch the application."
								else
									cd "$whereAmI"
									dbg "Installing bundled JRE successful."
								fi 
								chmod -R 744 "$DESTINATION_DIR" > /dev/null 2>&1
								J="$DESTINATION_DIR"
								J=`echo "$J" | sed 's/^[ ]*//;s/[ ]*$//;s/^[	]*//;s/[	]*$//'`
								echo "JVM_HOME:$J" >> "$DESTINATION_DIR/jvm"
								if [ -f "$DESTINATION_DIR/jvm" ] ; then 
									echo "$DESTINATION_DIR/jvm"
								else
									dbg "Installation of bundled JRE failed."	
								fi
							else
							 	dbg "diskspace check failed.Installation of bundled JRE failed." 	
							fi
				 			
				 		else
				 			dbg "Install Destination directory for bundled JRE is non-writable. Installation of bundled JRE failed."
				 		fi
				 	else
						dbg "Install Destination directory for bundled JRE is missing. Installation of bundled JRE failed. "
				 	fi
				else
					dbg "Bundled JRE file  $BUNDLED_JRE is missing. Installation of bundled JRE failed."
				fi 
				
			else
				dbg "unknown ACTION TAG"
			fi
		else
				dbg "ACTION TAG is missing in the input instructions file. JVM search can't be performed."
		fi
		cleanupandexit 0
	else
			dbg "Input instructions file missing. JVM Service operation failed."
	fi
}

handleApplicationExitStatus()
{
 	[	"$applicationExitStatus" ]  &&	[ \( -z "$SILENT" -a "$applicationExitStatus" -ne 0  \) ] && {
 	  pbl
 	  [ "$ismpVV" ] && applicationExitStatusMesg="The application is aborted with exit code $applicationExitStatus."
cat  << handleApplicationExitStatusSTOP







          $applicationExitStatusMesg
          






handleApplicationExitStatusSTOP
 	  
 		[ -f "$ISTEMP/APP_STDERR" ] && cat "$ISTEMP/APP_STDERR"
 	}
}

mode()
{
	mode=`awk 'END { i=index(a, "-is:in"); print i }' a="$1"  </dev/null 2>/dev/null`
	if [ "$mode" -ge 1 ] ; then
	 main "$@"
	fi 
}
_XPG=1  # to maintain backward compatibility with bourne shell on IRIX. See IRIX man pages of sh for more details.
BIN_SH=xpg4 # for True64 to use POSIX compliant shell.
mode "$@" 

setPreLaunchEnvironment()
{
dbg "setting up prelaunch environment"
}

expandDiskPartition()
{
echo 0
}

FILEINDEX0=05:00000000:000003F1:0000E800:Verify.jar:00:00000001:00000000
FILEINDEX1=01:00000001:00000475:0000EC00:jre.1.4.X.genericunix.jvm:00:00000001:00000000
FILEINDEX2=01:00000002:00000449:0000F400:9060252.tmp:00:00000001:00000000
FILEINDEX3=01:00000003:00000461:0000FC00:8688565.tmp:00:00000001:00000000
FILEINDEX4=01:00000004:000004E8:00010400:5580359.tmp:00:00000001:00000000
FILEINDEX5=01:00000005:00000453:00010C00:7840938.tmp:00:00000001:00000000
FILEINDEX6=01:00000006:00000461:00011400:5931409.tmp:00:00000001:00000000
FILEINDEX7=01:00000007:00000457:00011C00:4727690.tmp:00:00000001:00000000
FILEINDEX8=01:00000008:000004CA:00012400:2696463.tmp:00:00000001:00000000
FILEINDEX9=01:00000009:00000304:00012C00:5164634.tmp:00:00000001:00000000
FILEINDEX10=01:0000000A:000005AB:00013000:5104390.tmp:00:00000001:00000000
FILEINDEX11=01:0000000B:00000502:00013800:5671169.tmp:00:00000001:00000000
FILEINDEXCOUNT=12
Launch()
{
launcherParentDir=`dirname "$0"`
JAVA_ARGS_UNRESOLVED="%IF_EXISTS%("INIT_JAVA_HEAP", "@INIT_JAVA_HEAP@20m") %IF_EXISTS%("MAX_JAVA_HEAP", "@MAX_JAVA_HEAP@60m")"

resolveLaunchCommand "$RESOLVED_JVM"
if [ $resolve -ne 0 ]; then
 dbg "Launch Script can't not be resolved. Run the installer with -is:log <logfile> to view the errors."
cleanupandexit 3
else
pbmesg="Launching InstallShield Wizard"
pbc=8
pb
pbl
pbl
setPreLaunchEnvironment
JC="\"$JVM_HOME/$JVM_EXE\" $CLASSPATH_SWITCH .:$PREPEND_CLASSPATH:$CP1:"$JVM_CLASSPATH":$CP:\"/kozyrakis/users/sanchezd/zcache/SPECjbb2005/_uninst/uninstall.jar\":$INSTALLER_ARCHIVE:$EMBEDDED_JARS:$CP2:$CLASSPATH:$APPEND_CLASSPATH: -Dtemp.dir=\"$IS_TEMP_DIR\" -Dis.jvm.home=\"$IS_JVM_HOME\" -Dis.jvm.temp=\"$IS_JVM_TEMP\" -Dis.launcher.file=\"$IS_LAUNCHER_FILE\" -Dis.jvm.file=\"$IS_JVM_FILE\" "$RUNTIME_SYSTEMPROP" $JAVA_ARGS run "$app_args" 1>$APP_STDOUT 2>$APP_STDERR"
dbg LAUNCH_COMMAND="$JC"
eval $JC
applicationExitStatus=$?
fi
}
setpbmesg()
{
pbmesg="Initializing Universal Application Launcher"
}
#INSTRUCTIONS for the generic unix launcher.
Instructions()
{
VerifyResolvedJVM 2
if [ $resolvedJVMVerified -eq 0 ] ; then
Launch
else
searchJVM
if [ $sv -eq 0 ] ; then
Launch
fi
fi
}
main "$@"
######################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################PK
     ¹fö.            	  META-INF/þÊ  PK
     ¹fö.ª£ÝlD   D      META-INF/MANIFEST.MFManifest-Version: 1.0
Created-By: 1.2.2 (Sun Microsystems Inc.)

PK
     «fö.ÄcÉCY  Y     Verify.classÊþº¾  - '     ! " #
  
  
  	  
         %  &  ()Ljava/util/Properties; ()V &(Ljava/lang/String;)Ljava/lang/String; (Ljava/lang/String;)V ([Ljava/lang/String;)V 
1739021872 <init> Code LineNumberTable Ljava/io/PrintStream; 
SourceFile Verify Verify.java getProperties getProperty java/io/PrintStream java/lang/Object java/lang/System java/util/Properties main out println                       *· ±            	 $      N     &² 
¶ <§ ² 
¸ *2¶ 	¶ „*¾¡ÿë±               
   %       PK
 
     ¹fö.            	                META-INF/þÊ  PK
 
     ¹fö.ª£ÝlD   D                +   META-INF/MANIFEST.MFPK
 
     «fö.ÄcÉCY  Y               ¡   Verify.classPK      ·   $    ###############DESC:JRE 1.4.X
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*4*
/opt/*[jJ][rR][eE]*1*4*
/usr/*[jJ][dD][kK]*1*4*/jre
/opt/*[jJ][dD][kK]*1*4*/jre
/usr/*[jJ][aA][vV][aA]*1*4*
/opt/*[jJ][aA][vV][aA]*1*4*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ]2*
/usr/*[jJ]2*
/:
JVM_PROPERTIES:
java.version=1.4...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
JVM_HOME:/usr/java/j2sdk1.4.2_14/
SEARCH_ALL: 0
###########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.5.X
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*5*
/opt/*[jJ][rR][eE]*1*5*
/usr/*[jJ][dD][kK]*1*5*/jre
/opt/*[jJ][dD][kK]*1*5*/jre
/usr/*[jJ][aA][vV][aA]*1*5*
/opt/*[jJ][aA][vV][aA]*1*5*
/usr/*[jJ][sS][Ee]*
/usr/*[jJ][sS][Ee]*/jre
/usr/*[jJ][rR][Ee]*
/opt/*[jJ][sS][Ee]*
/opt/*[jJ][sS][Ee]*/jre
/opt/*[jJ][rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/usr/*[jJ][aA][vV][aA]*/*[jJ]*
/opt/*[jJ][aA][vV][aA]*/*[jJ]*
/opt/*[jJ]*
/usr/*[jJ]*
/:
JVM_PROPERTIES:
java.version=1.5...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
#######################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.4.0
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*4*0*
/opt/*[jJ][rR][eE]*1*4*0*
/usr/*[jJ][dD][kK]*1*4*0*/jre
/opt/*[jJ][dD][kK]*1*4*0*/jre
/usr/*[jJ][aA][vV][aA]*1*4*0*
/opt/*[jJ][aA][vV][aA]*1*4*0*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ]2*
/usr/*[jJ]2*
/:
JVM_PROPERTIES:
java.version=1.4.0...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
###############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.3.0
JVM_EXE: bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*3*0*
/opt/*[jJ][rR][eE]*1*3*0*
/usr/*[jJ][dD][kK]*1*3*0*/jre
/opt/*[jJ][dD][kK]*1*3*0*/jre
/usr/*[jJ][aA][vV][aA]*1*3*0*
/opt/*[jJ][aA][vV][aA]*1*3*0*
/usr/*[jJ][rR][eE]*1*3*
/opt/*[jJ][rR][eE]*1*3*
/usr/*[jJ][dD][kK]*1*3*/jre
/opt/*[jJ][dD][kK]*1*3*/jre
/usr/*[jJ][aA][vV][aA]*1*3*
/opt/*[jJ][aA][vV][aA]*1*3*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/:
JVM_PROPERTIES:
java.version=1.3.0...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.4.X
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*4*
/opt/*[jJ][rR][eE]*1*4*
/usr/*[jJ][dD][kK]*1*4*/jre
/opt/*[jJ][dD][kK]*1*4*/jre
/usr/*[jJ][aA][vV][aA]*1*4*
/opt/*[jJ][aA][vV][aA]*1*4*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ]2*
/usr/*[jJ]2*
/:
JVM_PROPERTIES:
java.version=1.4...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
#############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.4.1
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*4*1*
/opt/*[jJ][rR][eE]*1*4*1*
/usr/*[jJ][dD][kK]*1*4*1*/jre
/opt/*[jJ][dD][kK]*1*4*1*/jre
/usr/*[jJ][aA][vV][aA]*1*4*1*
/opt/*[jJ][aA][vV][aA]*1*4*1*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ]2*
/usr/*[jJ]2*
/:
JVM_PROPERTIES:
java.version=1.4.1...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
###############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.5.0
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*5*0*
/opt/*[jJ][rR][eE]*1*5*0*
/usr/*[jJ][dD][kK]*1*5*0*/jre
/opt/*[jJ][dD][kK]*1*5*0*/jre
/usr/*[jJ][aA][vV][aA]*1*5*0*
/opt/*[jJ][aA][vV][aA]*1*5*0*
/usr/*[jJ][sS][Ee]*
/usr/*[jJ][sS][Ee]*/jre
/usr/*[jJ][rR][Ee]*
/opt/*[jJ][sS][Ee]*
/opt/*[jJ][sS][Ee]*/jre
/opt/*[jJ][rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/usr/*[jJ][aA][vV][aA]*/*[jJ]*
/opt/*[jJ][aA][vV][aA]*/*[jJ]*
/opt/*[jJ]*
/usr/*[jJ]*
/:
JVM_PROPERTIES:
java.version=1.5.0...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
#########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.2.x
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*2*2*
/opt/*[jJ][rR][eE]*1*2*2*
/usr/*[jJ][dD][kK]*1*2*2*/jre
/opt/*[jJ][dD][kK]*1*2*2*/jre
/usr/*[jJ][aA][vV][aA]*1*2*2*
/opt/*[jJ][aA][vV][aA]*1*2*2*
/usr/*[jJ][rR][eE]*1*2*1*
/opt/*[jJ][rR][eE]*1*2*1*
/usr/*[jJ][dD][kK]*1*2*1*/jre
/opt/*[jJ][dD][kK]*1*2*1*/jre
/usr/*[jJ][aA][vV][aA]*1*2*1*
/opt/*[jJ][aA][vV][aA]*1*2*1*
/usr/*[jJ][rR][eE]*1*2*
/opt/*[jJ][rR][eE]*1*2*
/usr/*[jJ][dD][kK]*1*2*/jre
/opt/*[jJ][dD][kK]*1*2*/jre
/usr/*[jJ][aA][vV][aA]*1*2*
/opt/*[jJ][aA][vV][aA]*1*2*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/:
JVM_PROPERTIES:
java.version=1.2...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug
SEARCH_ALL: 0
SEARCH_ALL: 0
######################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.2.2
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*2*2*
/opt/*[jJ][rR][eE]*1*2*2*
/usr/*[jJ][dD][kK]*1*2*2*/jre
/opt/*[jJ][dD][kK]*1*2*2*/jre
/usr/*[jJ][aA][vV][aA]*1*2*2*
/opt/*[jJ][aA][vV][aA]*1*2*2*
/usr/*[jJ][rR][eE]*1*2*
/opt/*[jJ][rR][eE]*1*2*
/usr/*[jJ][dD][kK]*1*2*/jre
/opt/*[jJ][dD][kK]*1*2*/jre
/usr/*[jJ][aA][vV][aA]*1*2*
/opt/*[jJ][aA][vV][aA]*1*2*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/:
JVM_PROPERTIES:
"java.version=1.2.2"
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
SEARCH_ALL: 0
SEARCH_ALL: 0
############################################################################################################################################################################################################################################################DESC:JRE 1.3.X
JVM_EXE:bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*3*1*
/opt/*[jJ][rR][eE]*1*3*1*
/usr/*[jJ][dD][kK]*1*3*1*/jre
/opt/*[jJ][dD][kK]*1*3*1*/jre
/usr/*[jJ][aA][vV][aA]*1*3*1*
/opt/*[jJ][aA][vV][aA]*1*3*1*
/usr/*[jJ][rR][eE]*1*3*0*
/opt/*[jJ][rR][eE]*1*3*0*
/usr/*[jJ][dD][kK]*1*3*0*/jre
/opt/*[jJ][dD][kK]*1*3*0*/jre
/usr/*[jJ][aA][vV][aA]*1*3*0*
/opt/*[jJ][aA][vV][aA]*1*3*0*
/usr/*[jJ][rR][eE]*1*3*
/opt/*[jJ][rR][eE]*1*3*
/usr/*[jJ][dD][kK]*1*3*/jre
/opt/*[jJ][dD][kK]*1*3*/jre
/usr/*[jJ][aA][vV][aA]*1*3*
/opt/*[jJ][aA][vV][aA]*1*3*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/opt/*[jJ]2*
/usr/*[jJ]2*
/:
JVM_PROPERTIES:
java.version=1.3...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
#####################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################DESC:JRE 1.3.1
JVM_EXE: bin/java
PLATFORM_HINT:
JDK_HOME
JAVAHOME
JAVA_HOME
/:
PATH_HINT:
/usr/*[jJ][rR][eE]*1*3*1*
/opt/*[jJ][rR][eE]*1*3*1*
/usr/*[jJ][dD][kK]*1*3*1*/jre
/opt/*[jJ][dD][kK]*1*3*1*/jre
/usr/*[jJ][aA][vV][aA]*1*3*1*
/opt/*[jJ][aA][vV][aA]*1*3*1*
/usr/*[jJ][rR][eE]*1*3*
/opt/*[jJ][rR][eE]*1*3*
/usr/*[jJ][dD][kK]*1*3*/jre
/opt/*[jJ][dD][kK]*1*3*/jre
/usr/*[jJ][aA][vV][aA]*1*3*
/opt/*[jJ][aA][vV][aA]*1*3*
/usr/*[jJ][aA][vV][aA]*/*[jJ]2*
/opt/*[jJ][aA][vV][aA]*/*[jJ]2*
/usr/*[jJ]2[sS][Ee]*
/usr/*[jJ]2[sS][Ee]*/jre
/usr/*[jJ]2[rR][Ee]*
/opt/*[jJ]2[sS][Ee]*
/opt/*[jJ]2[sS][Ee]*/jre
/opt/*[jJ]2[rR][Ee]*
/usr/*[jJ][dD][kK]*/jre
/opt/*[jJ][dD][kK]*/jre
/usr/*[jJ][dD][kK]*
/opt/*[jJ][dD][kK]*
/usr/*[jJ][rR][eE]*
/opt/*[jJ][rR][eE]*
/usr/*[jJ][aA][vV][aA]*
/opt/*[jJ][aA][vV][aA]*
/opt/*[jJ]2*
/usr/*[jJ]2*
/:
JVM_PROPERTIES:
java.version=1.3.1...
/:
CLASSPATH:-cp
CLASSPATH_SEPARATOR::
SYSTEM:-D
SYSTEM_SEPARATOR:=
VERBOSE:-verbose
VERBOSE_GC:-verbose:gc
VERBOSE_CLASS:-verbose:class
VERBOSE_JNI:-verbose:jni
VERSION:-version
BOOT_CLASSPATH:-Xbootclasspath:
NO_CLASS_GC:-Xnoclassgc
INIT_JAVA_HEAP:-Xms
MAX_JAVA_HEAP:-Xmx
REDUCE_OS_SIGNALS:-Xrs
CHECK_JNI:-Xcheck:jni
RUNHPROF_HELP:-Xrunhprof:help
RUNHPROF_OPTION:-Xrunhprof
DEBUG:-Xdebug

SEARCH_ALL: 0
SEARCH_ALL: 0
##############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################