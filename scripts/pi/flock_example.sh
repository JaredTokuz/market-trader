#!/usr/bin/env bash

# Guanantees a single instance of this script is running at a time
# Additionally dependent script can run immediately during this script

[ "${FLOCKER}" != "$0" ] && exec env FLOCKER="$0" flock -en "$0" "$0" "$@" || :

./example.sh &

pid=$!
status=$?

if [ $status -eq 0 ]
then
    while ps -p $pid >/dev/null
    do
        ./nested/dependant.sh
        sleep 1
    done
fi

sleep 1

./nested/dependant.sh
