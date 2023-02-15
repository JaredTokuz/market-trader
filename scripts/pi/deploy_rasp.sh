#!/usr/bin/env bash

if [ "$#" -ne 1 ]
then
  echo "Usage: Pass the SSH host to deploy to"
  exit 1
fi

./scripts/pi/rasp_build.sh

SSH_HOST="$1"

scp -r ./dist "$SSH_HOST"
