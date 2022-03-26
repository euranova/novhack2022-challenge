#!/bin/bash

# Usage
# bash get_scoring_log.sh [folder where logs are saved]

# Example for 
# bash get_scoring_log.sh scoring_logs


if [ -z $1 ]
then
        echo "ERROR: You must run 'get_scoring_logs.sh $foler' where gfolder is the place to put your logs (the directory must exist)"
        exit 1
fi

LOG_GROUP_NAME="scoring"
FOLDER=$1


aws logs describe-log-streams --log-group-name ${LOG_GROUP_NAME} | jq -c '.logStreams[]' | while read LOG_STREAM; do
    LOG_STREAM_NAME=$(echo ${LOG_STREAM} | jq -r '.logStreamName' )
    aws logs get-log-events --log-group-name $LOG_GROUP_NAME --log-stream-name $LOG_STREAM_NAME > "${FOLDER}/${LOG_STREAM_NAME}.txt"
done
