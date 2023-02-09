#!/usr/bin/env bash
set -x
set -eo pipefail

DB_NAME="${DB_NAME:=tdameritrade}"

DB_PORT="${DB_PORT:=27017}"

# Allow to skip Docker if a dockerized Postgres database is already running
if [[ -z "${SKIP_DOCKER}" ]]
then
    docker run \
        -p "${DB_PORT}:27017" \
        -d \
        --name mongo \
        mongo
fi

# Keeping pinging Mongo until it's ready to accept commands
until docker exec mongo bash -c "mongosh --eval exit" > /dev/null 2>&1; do
    >&2 echo "Mongo is still unavailable - sleeping"
    sleep 1
done

>&2 echo "Mongo is up and running on port ${DB_PORT} - running migrations now!"

# Example command to run a migration 
# Databases are auto created when collections are first inserted
docker exec mongo bash -c "mongosh --eval use ${DB_NAME}";

>&2 echo "Mongo has been setup, ready to go!"
