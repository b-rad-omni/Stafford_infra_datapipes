#!/bin/bash
# docker/postgres/init-databases.sh

set -e
set -u

function create_user_and_database() {
    local database=$1
    local user=$2
    local password=$3
    echo "Creating user '$user' and database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        -- Create user if it doesn't exist
        DO
        \$\$
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_catalog.pg_user
                WHERE usename = '$user'
            ) THEN
                CREATE USER $user WITH PASSWORD '$password';
            END IF;
        END
        \$\$;

        -- Create database
        CREATE DATABASE $database;
        
        -- Grant all privileges
        GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
        
        -- Connect to the new database and set up permissions
        \c $database
        GRANT ALL ON SCHEMA public TO $user;
EOSQL
}

# Create databases based on POSTGRES_MULTIPLE_DATABASES environment variable
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    
    # Create Airflow database with its own user
    create_user_and_database "airflow" "airflow" "airflow"
    
    # Create Analytics database (for our data warehouse)
    create_user_and_database "analytics" "analytics" "analytics"
    
    # Create Superset database
    create_user_and_database "superset" "superset" "superset"
    
    # Create additional analytics schemas in the analytics database
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="analytics" <<-EOSQL
        -- Create schemas for different data layers
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE SCHEMA IF NOT EXISTS dbt_dev;
        
        -- Grant permissions
        GRANT ALL ON SCHEMA raw TO analytics;
        GRANT ALL ON SCHEMA staging TO analytics;
        GRANT ALL ON SCHEMA analytics TO analytics;
        GRANT ALL ON SCHEMA dbt_dev TO analytics;
        
        -- Set search path
        ALTER DATABASE analytics SET search_path TO analytics, staging, raw, public;
EOSQL
    
    echo "Multiple databases created successfully"
fi