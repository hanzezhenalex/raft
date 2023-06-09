#!/bin/sh

set -x

transferToHTML() {
    basePath=$1

    for file in "${basePath}"/*
    do
        if test -d $file
        then
            transferToHTML $file
        else
            python ./utils/logExplorer.py --log $file --out $file.html
        fi
    done
}

LOG_PATH="./logs"

if [ -d "${LOG_PATH}" ];then
  echo "start uploading logs..."
  transferToHTML $LOG_PATH
  sshpass -p "${SSH_PASSWD}" scp -o StrictHostKeyChecking=no -r $LOG_PATH "${USERNAME}"@"${SERVER}":~/tmp/push-val/"$SHA"
  exit 1
fi

