#!/bin/sh
## This is an example of what a run_multi.sh script might look like
##

date

java -fullversion

JVM=2

echo Starting Controller
java -cp jbb.jar:check.jar -Xms32m -Xmx32m spec.jbb.Controller -propfile SPECjbb.props &
sleep 5

x=1
y=`expr "$JVM" - 1`
while [ "$x" -le "$JVM" ]; do
    echo Starting instance $x
    java -cp jbb.jar:check.jar -Xms256m -Xmx256m spec.jbb.JBBmain -propfile SPECjbb.props -id $x > multi.$x &
    x=`expr "$x" + 1`
done

date

