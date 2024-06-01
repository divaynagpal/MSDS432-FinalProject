#!/bin/bash

# Define database connection details
DB_HOST="34.28.99.247"
DB_PORT="5432"
DB_NAME="ms432_project"
DB_USER="postgres"
DB_PASSWORD="your_database_password"

# Directory containing CSV files
CSV_DIR="/Users/supriyajadhav/Downloads"

# SQL commands
SQL_COMMANDS=$(cat <<EOF
CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL01"
(
    "Week Start" date,
    "Week Number" text,
    "ZIP" numeric,
    "Total Trip Count" numeric,
    "Weekly Positive Cases" numeric,
    "Severity" text,
	"Forecasted Severity" text
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL02"
(
    "Week Start" date,
    "Week Number" text,
    "From Airport" text,
    "To Zip Code" numeric,
    "Total Trip Count from Airport to Zip Code" bigint,
    "Weekly Positive Cases" numeric
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL03"
(
    "Week Number" text,
    "Week Start" date,
    "Community Area" numeric,
    "Number of Trips From Community Area" bigint,
    "Number of Trips To Community Area" bigint,
    "CCVI Category" text
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL04A"
(
    "Week Number" text,
    "Week Start" date,
    "ZIP" numeric,
    "Total Trip Count" numeric,
	"Forecasted Trip Count" integer
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL04B"
(
    "Month Number" text,
    "ZIP" numeric,
    "Total Trip Count" numeric,
	"Forecasted Trip Count" integer
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL04C"
(
    "Date" date,
    "ZIP" numeric,
    "Total Trip Count" numeric,
	"Forecasted Trip Count" integer
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL06"
(
    "ZIP" numeric,
    "Permit ID - ELIGIBLE FOR LOAN" text COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL05"
(
    "Community Area" numeric,
    "Total fees to be Waived" numeric
);

EOF
)

# Run the SQL commands
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$SQL_COMMANDS"

# Import data for each table
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL01\" FROM '$CSV_DIR/RPTTBL01.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL02\" FROM '$CSV_DIR/RPTTBL02.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL03\" FROM '$CSV_DIR/RPTTBL03.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL04A\" FROM '$CSV_DIR/RPTTBL04A.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL04B\" FROM '$CSV_DIR/RPTTBL04B.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL04C\" FROM '$CSV_DIR/RPTTBL04C.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL05\" FROM '$CSV_DIR/RPTTBL05.csv' DELIMITER ',' CSV HEADER;"
psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL06\" FROM '$CSV_DIR/RPTTBL06.csv' DELIMITER ',' CSV HEADER;"