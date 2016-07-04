#!/bin/bash 

if [ -z "${EASY_HOME}" ]; then
  export EASY_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ -z "${EASY_CONF_DIR}" ]; then
  export EASY_CONF_DIR="$EASY_HOME/conf"
fi

. "${EASY_HOME}/bin/load-easy-env.sh"

case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac

easy_conf=${EASY_CONF_DIR}/config.properties
logback_configurationFile=${EASY_CONF_DIR}/logback.xml
export LANG=en_US.UTF-8

if [ -f ${EASY_HOME}/bin/easyapi.pid ] ; then
	echo "found easyapi.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

str=`file -L ${RUNNER} | grep 64-bit`
if [ -n "$str" ]; then
	JAVA_OPTS="-server -Xms2048m -Xmx3072m -Xmn1024m -XX:SurvivorRatio=2 -XX:PermSize=96m -XX:MaxPermSize=256m -Xss256k -XX:-UseAdaptiveSizePolicy -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError"
else
	JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m "
fi

### Set app version
libFile=`ls ${EASY_HOME}/lib/ftc-api*`
libFile1=${libFile#*api-}
appVersion=${libFile1%-*}

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
EASY_OPTS="-DappName=easyapi -DappVersion=$appVersion -Dlogback.configurationFile=$logback_configurationFile -Deasy.conf=$easy_conf"

if [ -e ${easy_conf} -a -e ${logback_configurationFile} ]
then

	LAUNCH_CLASSPATH=${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar:${JAVA_HOME}/lib/calsses12.jar

	for i in ${EASY_HOME}/lib/*;
		do LAUNCH_CLASSPATH=${i}:"$LAUNCH_CLASSPATH";
	done

 	LAUNCH_CLASSPATH="${EASY_HOME}/conf:$LAUNCH_CLASSPATH";
 	
 	echo "cd to $bin_abs_path for workaround relative path"
  	cd ${bin_abs_path}
 	
	echo LOG CONFIGURATION : ${logback_configurationFile}
	echo Easy conf : ${easy_conf}
	echo CLASSPATH : ${LAUNCH_CLASSPATH}
	${RUNNER} ${JAVA_OPTS} ${EASY_OPTS} -classpath .:${LAUNCH_CLASSPATH} com.wanbo.easyapi.server.EasyServer 1>>${EASY_HOME}/logs/out.log 2>&1 &
	echo $! > ${EASY_HOME}/bin/easyapi.pid

else 
	echo "Easy conf("${easy_conf}") OR log configuration file($logback_configurationFile) is not exist,please create then first!"
fi
