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
            rm $file
        fi
    done
}

LOG_PATH="./logs"

if [ -d "${LOG_PATH}" ];then
  echo "start uploading logs..."
  transferToHTML $LOG_PATH
  sshpass -p "${SSH_PASSWD}" scp -r $LOG_PATH "${USERNAME}"@"${SERVER}":/tmp/push-val/"$HEAD_REF"."$SHA"
  exit 1
fi

