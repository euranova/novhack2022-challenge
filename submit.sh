#!/bin/bash

if [ -z $1 ]
then
        echo "ERROR: You must run 'submit.sh $team_name' where team_name is the name of your team"
        exit 1
fi

TEAM=$1

mvn clean package -f applications/pom.xml
sed "s/<artifactId>task1/<artifactId>task2/g" applications/pom.xml
sed "s/eu.euranova.novhack.MainTask1/eu.euranova.novhack.MainTask2/g" applications/pom.xml

BUCKET_NAME="novhack2022-submission-${TEAM}"

JAR_NAME="task1-1.0.jar"
OBJECT_NAME="${JAR_NAME}"
REMOTE_PATH="s3://$BUCKET_NAME/$OBJECT_NAME"
APP_NAME="Flink_${TEAM}"
LOCAL_PATH="applications/target/${JAR_NAME}"

aws s3 cp $LOCAL_PATH $REMOTE_PATH

mvn clean package -f applications/pom.xml
sed "s/<artifactId>task2/<artifactId>task1/g" applications/pom.xml
sed "s/eu.euranova.novhack.MainTask2/eu.euranova.novhack.MainTask1/g" applications/pom.xml

JAR_NAME="task2-1.0.jar"
OBJECT_NAME="${JAR_NAME}"
REMOTE_PATH="s3://$BUCKET_NAME/$OBJECT_NAME"
LOCAL_PATH="applications/target/${JAR_NAME}"

aws s3 cp $LOCAL_PATH $REMOTE_PATH
