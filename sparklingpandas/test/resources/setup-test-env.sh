#!/bin/bash
if [ -z "$SPARK_HOME" ]; then
   echo "$(tput setaf 1)Error: SPARK_HOME is not set, can not run tests.$(tput sgr0)"
   exit -1
fi

CURRENT_DIR=`pwd`
PROJECT_ROOT=`cd $x/../../../.. && pwd`

JARS=`ls $PROJECT_ROOT/target/scala-2.10/*.jar`
PYSPARK_SUBMIT_ARGS="--jars $JARS --driver-class-path $JARS pyspark-shell"
PYTHON_PATH=$PROJECT_ROOT:$PYHTON_PATH