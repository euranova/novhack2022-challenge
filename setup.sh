#!/bin/bash

# This script is the AWS CLI setup for the participants

echo -e 'Welcome to NovHack 2022!'
echo 'This script will set up your AWS credentials and check your connection'

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Download AWS CLI
echo 'Current AWS CLI version: '
aws --version
if [ $? -ne 0 ]; then
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"  && \
  unzip awscliv2.zip && \
  sudo ./aws/install
fi

echo 'Please provide your team name (team1 for example): '
echo 'Team name: '
read -r TEAM_NAME

echo 'Please provide your AWS credentials: '
echo 'AWS Access Key ID: '
read -r AWS_ACCESS_KEY_ID

echo 'AWS Secret Access Key: '
read -r AWS_SECRET_ACCESS_KEY


export AWS_REGION=eu-west-3
export AWS_OUTPUT=json

aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
aws configure set region $AWS_REGION
aws configure set output $AWS_OUTPUT
# Use the default profile name

# Test if credentials are valid
aws sts get-caller-identity
if [ $? -eq 0 ]; then
  echo "Success: AWS credentials correctly set up!"
else
  echo "Failure: Invalid AWS credentials"
  exit 1
fi

# Pull config file from s3
FILE_NAME="s3://novhack2022-application-configurations/application_properties_${TEAM_NAME}.properties"
aws s3 cp "${FILE_NAME}" ${SCRIPT_DIR}/applications/src/main/resources/application.properties
if [ $? -eq 0 ]; then
  echo "Success: Config file downloaded!"
else
  echo "Failure: Invalid team name"
  exit 1
fi

# Download historical data
aws s3 cp --recursive s3://novhack2022-client-data/prod/history ${SCRIPT_DIR}/data
if [ $? -eq 0 ]; then
  echo "Success: Historical data downloaded!"
else
  echo "Failure: failed to download the historical data"
  exit 1
fi

# Get the sql script from S3
FILE_NAME="s3://novhack2022-sql-scripts/init_db.sql"
aws s3 cp "${FILE_NAME}" ${SCRIPT_DIR}/novhack-dev-env/database/init_db.sql
if [ $? -eq 0 ]; then
  echo "Success: Historical data downloaded!"
else
  echo "Failure: failed to download the init sql script"
  exit 1
fi

# Connexion with the database
echo 'Current psql version: '
psql --version
if [ $? -ne 0 ]; then
  sudo apt-get install postgresql
fi

./database/init-db.sh
if [ $? -eq 0 ]; then
  echo "Success: Database initiated !"
else
  echo "Failure: Failed to initiate the PostgreSQL database"
  exit 1
fi

echo -e 'You are ready to use the code!\nGood luck! '
