#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

PROPERTY_FILE=${SCRIPT_DIR}/../applications/src/main/resources/application.properties

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=$(cat $PROPERTY_FILE | grep "$PROP_KEY" | cut -d' = ' -f2 | xargs)
   echo "${PROP_VALUE}"
}


# Variables assignations
REGION="eu-west-3"
SSL_ROOT_CERT_PATH="${SCRIPT_DIR}/eu-west-3-bundle.pem"
COMPLETE_HOSTNAME=$(cat $PROPERTY_FILE | grep "jdbc.hostname" | cut -d'=' -f2)
PORT=$(echo "${COMPLETE_HOSTNAME}" | rev | cut -d: -f1 | rev)
RDS_HOST=$(echo "${COMPLETE_HOSTNAME}" | cut -d: -f1 )
DATABASE=$(cat $PROPERTY_FILE | grep "jdbc.database" | cut -d'=' -f2)
USERNAME=$(cat $PROPERTY_FILE | grep "jdbc.username" | cut -d'=' -f2)
PASSWORD=$(cat $PROPERTY_FILE | grep "jdbc.password" | cut -d'=' -f2)

# Connection with the postgresql database and Run commands in postgresql
psql "host=${RDS_HOST//[[:space:]]/} port=${PORT//[[:space:]]/} sslmode=verify-full sslrootcert=${SSL_ROOT_CERT_PATH} dbname=${DATABASE//[[:space:]]/} user=${USERNAME//[[:space:]]/} password=${PASSWORD//[[:space:]]/}"
