#!/bin/bash
set -e

echo "Initializing postgres for chainstorage..."
exec postgres \
    -c ssl=on \
    -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
    -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key \
    "$@" 

echo "pg started" 

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
  -- Create the chainstorage role if it doesn't exist
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'chainstorage') THEN
      CREATE ROLE chainstorage WITH LOGIN PASSWORD 'chainstorage';
    END IF;
  END
  $$;

  -- Grant necessary permissions to chainstorage role
  ALTER ROLE chainstorage CREATEDB;
EOSQL
