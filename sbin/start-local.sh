# master node is local, UI at spark://SMBP:8080
$SPARK_HOME/sbin/start-master.sh
# launch 2 executors locally
SPARK_WORKER_INSTANCES=2 SPARK_WORKER_CORES=1 $SPARK_HOME/sbin/start-slave.sh spark://SMBP:7077
