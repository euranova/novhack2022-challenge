#!/bin/bash

# File to push on S3
FILE_NAME="custom_tables.sql"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROPERTY_FILE=${SCRIPT_DIR}/../applications/src/main/resources/application_properties.properties

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=$(cat $PROPERTY_FILE | grep "$PROP_KEY" | cut -d' = ' -f2 | xargs)
   echo "${PROP_VALUE}"
}

TEAM_NAME=$(cat $PROPERTY_FILE | grep "jdbc.database" | cut -d'=' -f2)

aws s3 cp ${SCRIPT_DIR}/${FILE_NAME} "s3://novhack2022-sql-scripts/${TEAM_NAME//[[:space:]]/}/${FILE_NAME}"
if [ $? -eq 0 ]; then
  echo "Success: SQL request file uploaded!"
else
  echo "Failure!" 
  exit 1
fi