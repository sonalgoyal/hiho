#!/bin/bash

exec ${HADOOP_HOME}/bin/hadoop jar ../lib/hiho.jar co.nubetech.hiho.job.DBQueryInputJob -conf $0

