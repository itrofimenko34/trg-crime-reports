#!/bin/sh

set -Eeuo pipefail

WORK_DIR=`pwd`
JAR_DIR="$WORK_DIR/jar"
JAR_PATH="$JAR_DIR/trg-crime-reports.jar"
LOG4J_PATH="$WORK_DIR/target/classes/log4j.properties"

usage() {
  cat << EOF
trg-crime-reports.sh supports next tasks:
  export                  Launches export Spark job that reads crimes(street) and outcomes csv files and persist
                          result in parquet format.
  kpi                     Launches export Spark job that reads crimes(street) and outcomes csv files and persist
                          result in parquet format.
  query                   Execute SQL query in spark-sql console. Can be used to interact with parquet files.
  start                   Build the project and starts spark in docker container. Takes input data path as a parameter.
  stop                    Stop the container and do the clean up.
  restart                 Rebuilds the jar and restarts docker container.
  update-jar              Rebuilds the jar
EOF
  exit
}

msg() {
  echo >&2 "${1-}"
}

update_jar(){
    msg "Building new artifact..."
    sbt clean package
    mkdir -p $JAR_DIR
    cp "$WORK_DIR/target/trg-crime-reports-0.1.jar" $JAR_PATH
}

container_start(){
    INPUT_DIR="${1-}"

    update_jar

    cp docker-compose.yaml.template docker-compose.yaml
    sed -i "" "s|<INPUT_DIR>|$INPUT_DIR|g" docker-compose.yaml
    sed -i "" "s|<JAR_PATH>|$JAR_PATH|g" docker-compose.yaml
    sed -i "" "s|<LOG4J_PATH>|$LOG4J_PATH|g" docker-compose.yaml

    docker compose up -d
}

container_stop(){
    docker compose down
    rm docker-compose.yaml
}

launch_spark_batch_job(){
    CONTAINER_ID=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

    if [ -z "$CONTAINER_ID" ]; then
      msg "Spark master container is not found. Try: ./trg-crime-reports.sh start"
      exit
    fi

    msg "Submitting spark job..."

    docker exec $CONTAINER_ID ./spark/bin/spark-submit --class "${1-}" \
    --driver-memory 512M \
    --executor-memory 2G \
    /jar/trg-crime-reports.jar \
    "$@" master="spark://spark-master:7077"
}

if [ -n "$1" ]; then
  TASK=$1
else
    msg "TASK is not specified."
  usage
fi

if [ "$TASK" == "start" ]; then
    if [ -n "$2" ]; then
          container_start $2
    else
        msg "Path to the csv data is not specified."
      usage
    fi
elif [ "$TASK" == "stop" ]; then
    container_stop
elif [ "$TASK" == "restart" ]; then
    docker compose down
    docker compose up -d
elif [ "$TASK" == "update-jar" ]; then
    update_jar
elif [ "$TASK" == "export" ]; then
    launch_spark_batch_job trg.ParquetExporter "$@" inputPath=data
elif [ "$TASK" == "kpi" ]; then
  launch_spark_batch_job trg.KPIProcessor "$@" inputPath=data/parquet
elif [ "$TASK" == "query" ]; then
    if [ -z "$2" ]; then
      msg 'Provide SQL query. Sample: ./trg-crime-reports.sh query "SELECT * FROM data LIMIT 10;"'
      exit
    fi
    USER_QUERY=$2

    CONTAINER_ID=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

    if [ -z "$CONTAINER_ID" ]; then
      msg "Spark master container is not found. Try: ./trg-crime-reports.sh start"
      exit
    fi
    msg "Starting spark-shell"

  TEMP_VIEW_QUERY="CREATE TEMPORARY VIEW data USING org.apache.spark.sql.json OPTIONS (path 'data/parquet/*.json');"
  msg "$TEMP_VIEW_QUERY $USER_QUERY"
  docker exec $CONTAINER_ID ./spark/bin/spark-sql --conf "spark.hadoop.hive.cli.print.header=true" -e "$TEMP_VIEW_QUERY $USER_QUERY"

else
  msg "TASK is not recognized."
  usage
fi

exit