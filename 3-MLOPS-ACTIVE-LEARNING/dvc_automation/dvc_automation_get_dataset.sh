#!/bin/bash
# This bash script is used to get the Dataset and model weights  to DVC for the the given branch and TAG
export GITHUB_URL=$1
export BRANCH_NAME=$2
export REPO_NAME=$3
export S3_URL=$4
export TAG=$5

echo 'GITHUB_URL =' $GITHUB_URL
echo 'BRANCH_NAME =' $BRANCH_NAME
echo 'REPO_NAME =' $REPO_NAME
echo 'S3_URL =' $S3_URL
echo 'TAG =' $TAG
echo 'git clone main'
git clone --branch $BRANCH_NAME  $GITHUB_URL
cd $REPO_NAME

# Check if the branch exists in the local repository




#


if [ ! -d ".dvc" ]
then
dvc init
# dvc remote add storage $S3_URL
dvc remote add -d microteksremote $S3_URL/DVC_STORAGE
fi

# modify for your credentials absolute path
dvc remote modify microteksremote endpointurl https://fra1.digitaloceanspaces.com/
dvc remote modify microteksremote region fra1
dvc remote modify --local microteksremote access_key_id ACCESS_KEY
dvc remote modify --local microteksremote secret_access_key SECRET_ACCESS

git checkout  $TAG

dvc pull  -r microteksremote


