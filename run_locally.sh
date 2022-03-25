#!/bin/bash

cd "/workspace/novhack-dev-env"
mvn clean package -f "/workspace/novhack-dev-env/applications/pom.xml" -DTASKID=task1 -Dmain.task=eu.euranova.novhack.Main

jobID=$(./flink-1.13.2/bin/flink list | grep Main | awk '{print $4}')
if [[ ! -z "$jobID" ]]
then 
    ./flink-1.13.2/bin/flink cancel $jobID
fi
./flink-1.13.2/bin/flink run "/workspace/novhack-dev-env/applications/target/task1-1.0.jar"