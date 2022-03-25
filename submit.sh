#!/bin/bash

mvn clean package -f applications/pom.xml -DTASKID=task1 -Dmain.task=eu.euranova.novhack.MainTask1

LOCAL_PATH="applications/target/${JAR_NAME}"
TEAM="team1"
BUCKET_NAME="novhack2022-submission-${TEAM}"

JAR_NAME="task1-1.0.jar"
OBJECT_NAME="${JAR_NAME}"
REMOTE_PATH="s3://$BUCKET_NAME/$OBJECT_NAME"
APP_NAME="Flink_${TEAM}"

#aws s3 cp $LOCAL_PATH $REMOTE_PATH

mvn clean package -f applications/pom.xml -DTASKID=task2 -Dmain.task=eu.euranova.novhack.MainTask2

JAR_NAME="task2-1.0.jar"
OBJECT_NAME="${JAR_NAME}"
REMOTE_PATH="s3://$BUCKET_NAME/$OBJECT_NAME"

#aws s3 cp $LOCAL_PATH $REMOTE_PATH
