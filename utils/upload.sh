#!/bin/sh

HEAD_REF=$1 # ${{ github.head_ref }}
SHA=$2 # ${{ github.sha }}
USENAME=$3
SERVER=$4
SSH_KEY=$5

transferToHTML() {
    basePath=$1

    for file in "${basePath}"/*
    do
        if test -d $file
        then
            transferToHTML $file
        else
            python ./utils/LogExplore.py --log $file --out $file.html
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

  scp -i $PEM_FILE -r $LOG_PATH "${USENAME}"@"${SERVER}":/tmp/push-val/"$HEAD_REF"."$SHA"
fi

