#!/bin/bash

JAR_NAME="applications-1.0.jar"
LOCAL_PATH="target/${JAR_NAME}"
TEAM="team1"

BUCKET_NAME="novhack2022-submission-${TEAM}"
OBJECT_NAME="${JAR_NAME}"
REMOTE_PATH="s3://$BUCKET_NAME/$OBJECT_NAME"
APP_NAME="Flink_${TEAM}"

#aws s3 cp $LOCAL_PATH $REMOTE_PATH

CREATE_TS=$(aws kinesisanalyticsv2 describe-application --application-name "${APP_NAME}" --query 'ApplicationDetail.CreateTimestamp')

aws kinesisanalyticsv2 delete-application \
    --application-name "${APP_NAME}" \
    --create-timestamp "${CREATE_TS}"

aws kinesisanalyticsv2 start-application --application-name ${APP_NAME}
