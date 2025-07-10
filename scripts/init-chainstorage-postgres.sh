#!/bin/sh
set -e

echo "Setting up PostgreSQL for Chainstorage..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
  -- Create chainstorage roles
  CREATE ROLE chainstorage_worker WITH LOGIN PASSWORD 'chainstorage_worker';
  CREATE ROLE chainstorage_server WITH LOGIN PASSWORD 'chainstorage_server';

  -- Create chainstorage databases
  CREATE DATABASE chainstorage_ethereum_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_bitcoin_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_base_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_arbitrum_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_polygon_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_solana_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_aptos_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_avacchain_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_bsc_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_fantom_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_optimism_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_tron_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_story_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_dogecoin_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_litecoin_mainnet OWNER chainstorage_worker;
  CREATE DATABASE chainstorage_bitcoincash_mainnet OWNER chainstorage_worker;

  -- Grant permissions to server role (read-only access)
  GRANT CONNECT ON DATABASE chainstorage_ethereum_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_bitcoin_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_base_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_arbitrum_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_polygon_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_solana_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_aptos_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_avacchain_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_bsc_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_fantom_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_optimism_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_tron_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_story_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_dogecoin_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_litecoin_mainnet TO chainstorage_server;
  GRANT CONNECT ON DATABASE chainstorage_bitcoincash_mainnet TO chainstorage_server;
EOSQL

echo "✅ Chainstorage PostgreSQL setup complete!"
echo "Databases: All chainstorage blockchain databases"
echo "Roles: chainstorage_worker (rw), chainstorage_server (ro)" 