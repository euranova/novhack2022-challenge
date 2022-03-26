#!/bin/bash

# Usage
# bash get_logs.sh [folder where logs are saved]

# Example for 
# bash get_logs.sh 3 logs

PROPERTY_FILE=${SCRIPT_DIR}/src/main/resources/application_properties.properties

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=$(cat $PROPERTY_FILE | grep "$PROP_KEY" | cut -d' = ' -f2 | xargs)
   echo "${PROP_VALUE}"
}

TEAM_NAME=$(cat $PROPERTY_FILE | grep "jdbc.database" | cut -d'=' -f2)

LOG_GROUP_NAME="novhack2022-kda-${TEAM_NAME//[[:space:]]/}"
LOG_STREAM_NAME_1="novhack2022-task1-kda-log-stream-${TEAM_NAME//[[:space:]]/}"
LOG_STREAM_NAME_2="novhack2022-task2-kda-log-stream-${TEAM_NAME//[[:space:]]/}"
FOLDER=$1

aws logs get-log-events --log-group-name $LOG_GROUP_NAME --log-stream-name $LOG_STREAM_NAME_1 > $FOLDER"/task1.json"
aws logs get-log-events --log-group-name $LOG_GROUP_NAME --log-stream-name $LOG_STREAM_NAME_2 > $FOLDER"/task2.json"