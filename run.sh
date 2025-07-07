#!/bin/bash

# Variables
PGUSER=postgres
PGPASSWORD=1234
PGDATABASE=mariage_db
PGHOST=localhost
PGPORT=5432

export PGPASSWORD=$PGPASSWORD


RESULT=$(psql -U $PGUSER -h $PGHOST -p $PGPORT -tAc "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE'")
if [ "$RESULT" != "1" ]; then
  echo "Creating $PGDATABASE db..."
  createdb -U $PGUSER -h $PGHOST -p $PGPORT $PGDATABASE
  echo "done."
else
  echo "Base $PGDATABASE ok."
fi
