#!/usr/bin/sh

DIRECTORY="/home/jt/Projects/angular-trader/dist"

if [ ! -d "$DIRECTORY" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  echo "Directory does not exist"
  exit 1
fi

cp -r $DIRECTORY/* .;