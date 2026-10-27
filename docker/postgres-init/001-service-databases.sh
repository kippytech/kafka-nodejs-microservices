#!/bin/bash
set -euo pipefail

for service in auth task media workflow; do
  password="${service}_dev_password"
  role="${service}_service"
  database="${service}_db"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set=role="$role" --set=password="$password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role', :'password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role')\gexec
SQL
  exists="$(psql -tAc "SELECT 1 FROM pg_database WHERE datname='${database}'" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB")"
  if [ "$exists" != "1" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
      --command="CREATE DATABASE ${database} OWNER ${role}"
  fi
done
