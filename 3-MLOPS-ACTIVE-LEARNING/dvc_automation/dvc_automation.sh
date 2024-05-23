#!/bin/bash
# This bash script is used to push Dataset and model weights  to DVC
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
git clone  $GITHUB_URL
cd $REPO_NAME
echo 'git checkout'
# Check if the branch exists in the local repository
BRANCH='my-branch-name'
git ls-remote --exit-code --heads origin $BRANCH_NAME >/dev/null 2>&1
EXIT_CODE=$?

if [[ $EXIT_CODE == '0' ]]; then
  echo "Git branch '$BRANCH_NAME' exists in the remote repository"
  git checkout $BRANCH_NAME
elif [[ $EXIT_CODE == '2' ]]; then
  echo "Git branch '$BRANCH_NAME' does not exist in the remote repository"
  git checkout -b $BRANCH_NAME
  git push -u origin $BRANCH_NAME
fi



cd ..
cp -rf model $REPO_NAME 
cp -rf data_model $REPO_NAME

# modify for your repo name
cd $REPO_NAME

if [ ! -d ".dvc" ]
then
dvc init
# dvc remote add storage $S3_URL
dvc remote add -d microteksremote $S3_URL/DVC_STORAGE
fi

# modify for your credentials absolute path
dvc remote modify microteksremote endpointurl https://fra1.digitaloceanspaces.com/
dvc remote modify microteksremote region fra1

dvc remote modify --local microteksremote access_key_id SECRET_ACCESS
dvc remote modify --local microteksremote secret_access_key SECRET_ACCESS

# This is the code that runs on the microteks remote. We're going to make sure that the version_aware flag is set to true
dvc remote modify microteksremote version_aware true
# Add data_model to DVC. This is called by dvc. init
dvc add data_model
# Add  model folder to DVC . 
dvc add model
# Add current folder to the repository. 
git add .
# Commits the current branch to the repository.
git commit -m "$TAG"
# Tag the repository 
git tag $TAG
# Checkout the folder using DVC
dvc checkout
# Push changes to remote repository. 
git push -u origin $BRANCH_NAME $TAG
# Push microteks to dvc and wait for it to complete. 
dvc push -r microteksremote

cd ..
rm -rf $REPO_NAME

