#!/bin/sh

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

  PEM_FILE="key.pem"
  echo $SSH_KEY | tr -d '\r' > $PEM_FILE
  chmod 400 $PEM_FILE

  scp -i $PEM_FILE -r $LOG_PATH "${USERNAME}"@"${SERVER}":/tmp/push-val/"$HEAD_REF"."$SHA"

  exit 1
fi

