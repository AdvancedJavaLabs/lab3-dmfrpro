#!/usr/bin/env bash

echo "Starting cluster..."
docker-compose up -d
sleep 5

CONTAINER=$(docker-compose ps -q namenode)

echo "Setting up HDFS..."
docker exec $CONTAINER bash -c "
    # Clean everything
    hdfs dfs -rm -r /sales_input /sales_temp /sales_result 2>/dev/null || true
    
    # Create and upload to sales_input
    hdfs dfs -mkdir -p /sales_input
    for f in /opt/hadoop-project/input/*.csv; do
        if [ -f \"\$f\" ]; then
            hdfs dfs -put \"\$f\" /sales_input/
        fi
    done
    
    echo 'Uploaded files:'
    hdfs dfs -ls /sales_input
"

echo "Compiling..."
mvn clean package

echo "Running job..."
docker exec $CONTAINER bash -c "
    yarn jar /opt/hadoop-project/target/lab3-dmfrpro-1.0-SNAPSHOT-jar-with-dependencies.jar \
        org.ifmo.app.SalesAnalysisDriver \
        /sales_input \
        /sales_temp \
        /sales_result
"

echo -e "\n=== RESULTS ==="
docker exec $CONTAINER hdfs dfs -cat /sales_result/part-r-00000
