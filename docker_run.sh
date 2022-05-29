#!/bin/bash
JENKINSE_DIR=/var/lib/jenkins/workspace
if [ $(dirname $PWD) = $JENKINSE_DIR ]; then
    #jenkinsから実行される場合
    source /project/horse-data-scraper/.env
    ENV_FILE_PATH=/project/horse-data-scraper/.env
else
    #直接実行される場合
    source $PWD/.env
    ENV_FILE_PATH=$PWD/.env
fi
docker run --env-file=$ENV_FILE_PATH --mount type=bind,source=$MOUNT_POINT,target=$MOUNT_POINT horse-html-scraper