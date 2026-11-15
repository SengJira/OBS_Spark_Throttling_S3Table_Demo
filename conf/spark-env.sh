#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_MASTER_HOST=192.168.1.40
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=3g
export SPARK_DAEMON_MEMORY=1g
export SPARK_PID_DIR=/var/run/spark
export SPARK_LOG_DIR=/var/log/spark
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///var/log/spark-events -Dspark.history.ui.port=18080"

# worker work-dir cleanup (executor dirs were filling the disk)
SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=600 -Dspark.worker.cleanup.appDataTtl=1800"
