#!/bin/bash

# Database connection parameters
DB_NAME="ms432_project"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
DB_PASSWORD="your_database_password"

# SQL commands
SQL_COMMANDS=$(cat <<EOF
------------------------------------------------------------------------------------------
-- Create Staging zone TRIPS_DATA table --------------------------------------------------
-- Identify ZIP by performing spatial join between ---------------------------------------
-- LZ.TRIPS_COMBINED and LZ.BUNDARIES_ZIP on centroid location and zip geom --------------
-- Impute missing community area by performing spatial join between ----------------------
-- LZ.TRIPS_COMBINED and LZ.COMM_AREA on community area location and comm area the_geom --
-- Filter out records not having pickup and dropoff centroid location --------------------
-- Load data in staging zone TAXI_TRIPS table --------------------------------------------
------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."STG.TRIPS_DATA"
(
    "Trip ID" text,
    "Trip Start Timestamp" date,
    "Week Start" date,
    "Pickup Community Area" numeric,
    "Dropoff Community Area" numeric,
    "Pickup Centroid Latitude" numeric,
    "Pickup Centroid Longitude" numeric,
    "Pickup Centroid Location" geometry(Point,4326),
    "Dropoff Centroid Latitude" numeric,
    "Dropoff Centroid Longitude" numeric,
    "Dropoff Centroid Location" geometry(Point,4326),
    "Pickup Zip" numeric,
    "Dropoff Zip" numeric,
    "Week Number" text,
    "Month Number" text
)

-- INSERT INTO statement
INSERT INTO public."STG.TRIPS_DATA" (
    "Trip ID",
    "Trip Start Timestamp",
    "Week Start",
    "Pickup Community Area",
    "Dropoff Community Area",
    "Pickup Centroid Latitude",
    "Pickup Centroid Longitude",
    "Pickup Centroid Location",
    "Dropoff Centroid Latitude",
    "Dropoff Centroid Longitude",
    "Dropoff Centroid Location",
    "Pickup Zip",
    "Dropoff Zip",
    "Week Number",
    "Month Number"
)
SELECT
    t."Trip ID",
    t."Trip START Timestamp" AS "Trip Start Timestamp",
	t."Week Start",
	CASE
		WHEN t."Pickup Community Area" IS NOT NULL AND t."Pickup Community Area" <> 0 THEN t."Pickup Community Area"
 		ELSE ca1."AREA_NUMBE"
		END AS "Pickup Community Area",
	CASE
		WHEN t."Dropoff Community Area" IS NOT NULL AND t."Dropoff Community Area" <> 0 THEN t."Dropoff Community Area"
 		ELSE ca2."AREA_NUMBE"
		END AS "Dropoff Community Area",
    t."Pickup Centroid Latitude",
    t."Pickup Centroid Longitude",
    t."Pickup Centroid Location",
    t."Dropoff Centroid Latitude",
    t."Dropoff Centroid Longitude",
    t."Dropoff Centroid Location",
    bz1."ZIP" AS "Pickup Zip",
    bz2."ZIP" AS "Dropoff Zip",
	(EXTRACT(WEEK FROM t."Week Start ISO")::text || '-' || EXTRACT(YEAR FROM t."Week Start ISO")::text) AS "Week Number",
    (EXTRACT(MONTH FROM t."Trip START Timestamp")::text || '-' || EXTRACT(YEAR FROM t."Trip START Timestamp")::text) AS "Month Number"
FROM
    public."LZ.TAXI_TRIPS" t
    LEFT JOIN public."LZ.BOUNDARIES_ZIP" bz1 ON ST_Within(t."Pickup Centroid Location", bz1."the_geom")
    LEFT JOIN public."LZ.BOUNDARIES_ZIP" bz2 ON ST_Within(t."Dropoff Centroid Location", bz2."the_geom")
	LEFT JOIN public."LZ.COMM_AREA" ca1 ON ST_Within(t."Pickup Centroid Location", ca1."the_geom")
	LEFT JOIN public."LZ.COMM_AREA" ca2 ON ST_Within(t."Dropoff Centroid Location", ca2."the_geom")
WHERE
	t."Pickup Centroid Location" IS NOT NULL
	OR t."Dropoff Centroid Location" IS NOT NULL;

--------------------------------------------------------
-- Create Staging zone COVID_BY_ZIP table --------------
-- Filter out records not having zip and zip location --
-- Load data in staging zone COVID_BY_ZIP table --------
-- from LZ.COVID_BY_ZIP table --------------------------
--------------------------------------------------------
CREATE TABLE IF NOT EXISTS public."STG.COVID_BY_ZIP"
(
    "ZIP Code" text,
    "Week Start" date,
    "Cases - Weekly" numeric,
    "Cases - Cumulative" numeric,
    "Tests - Weekly" numeric,
    "Tests - Cumulative" numeric,
    "Week Number" text,
    "Month Number" text
)

-- INSERT INTO statement
INSERT INTO public."STG.COVID_BY_ZIP" (
    "ZIP Code",
    "Week Start",
    "Cases - Weekly",
    "Cases - Cumulative",
    "Tests - Weekly",
    "Tests - Cumulative",
    "Week Number",
    "Month Number"
)
SELECT
    "ZIP Code",
    "Week Start",
    "Cases - Weekly",
    "Cases - Cumulative",
    "Tests - Weekly",
    "Tests - Cumulative",
    (EXTRACT(WEEK FROM "Week Start ISO")::text || '-' || EXTRACT(YEAR FROM "Week Start ISO")::text) AS "Week Number",
    (EXTRACT(MONTH FROM "Week Start")::text || '-' || EXTRACT(YEAR FROM "Week Start")::text) AS "Month Number"
FROM
	public."LZ.COVID_BY_ZIP"
WHERE 
	("ZIP Code" IS NOT NULL AND "ZIP Code" <> 'Unknown')
	AND "ZIP Code Location" IS NOT NULL;

------------------------------------------------
-- Create Staging zone CCVI_BY_CA table --------
-- Filter out records with zip code entries ----
-- Load data in staging zone CCVI_BY_CA table --
-- from LZ.CCVI_BY_CA table --------------------
------------------------------------------------

CREATE TABLE IF NOT EXISTS public."STG.CCVI_BY_CA"
(
    "Community Area" numeric,
    "CCVI Category" text
)

-- INSERT INTO statement
INSERT INTO public."STG.CCVI_BY_CA" (
    "Community Area",
    "CCVI Category"
)
SELECT 
    "Community Area or ZIP Code"::numeric AS "Community Area",
    "CCVI Category"
FROM 
    public."LZ.CCVI_BY_CA"
WHERE
    TRIM("Geography Type") = 'CA';

-------------------------------------------------------------------------------------------
-- Create Staging zone BUILDING_PERMIT table ----------------------------------------------
-- Identify ZIP by performing spatial join between ----------------------------------------
-- LZ.BUILDING_PERMIT and LZ.BUNDARIES_ZIP on community location and zip geom -------------
-- Impute missing community area by performing spatial join between -----------------------
-- LZ.BUILDING_PERMIT and LZ.COMM_AREA on community area location and comm area the_geom --
-- Filter out records not having community centroid location ------------------------------
-- Load data in staging zone BUILDING_PERMIT table from LZ.BUILDING_PERMIT table ----------
-------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."STG.BUILDING_PERMIT"
(
    "ID" text,
    "PERMIT#" text,
    "PERMIT_TYPE" text,
    "SUBTOTAL_UNPAID" numeric,
    "LATITUDE" numeric,
    "LONGITUDE" numeric,
    "LOCATION" geometry(Point,4326),
    "COMMUNITY_AREA" numeric,
    "ZIP" numeric
)

-- INSERT INTO statement
INSERT INTO public."STG.BUILDING_PERMIT" (
    "ID",
    "PERMIT#",
    "PERMIT_TYPE",
    "SUBTOTAL_UNPAID",
    "LATITUDE",
    "LONGITUDE",
    "LOCATION",
    "COMMUNITY_AREA",
    "ZIP"
)
SELECT 
    bp."ID",
    bp."PERMIT#",
    bp."PERMIT_TYPE",
    bp."SUBTOTAL_UNPAID"::numeric,
    bp."LATITUDE",
    bp."LONGITUDE",
    bp."LOCATION",
    CASE
        WHEN bp."COMMUNITY_AREA" IS NOT NULL AND bp."COMMUNITY_AREA" <> 0 THEN bp."COMMUNITY_AREA"
        ELSE ca."AREA_NUMBE"
    END AS "COMMUNITY_AREA",
    bz."ZIP"
FROM 
    public."LZ.BUILDING_PERMIT" bp
    LEFT JOIN public."LZ.BOUNDARIES_ZIP" bz ON ST_Within(bp."LOCATION", bz."the_geom")
    LEFT JOIN public."LZ.COMM_AREA" ca ON ST_Within(bp."LOCATION", ca."the_geom")
WHERE
    bp."LOCATION" IS NOT NULL;

---------------------------------------------------
-- Create Staging zone PUBLIC_HEALTH table --------
-- Load data in staging zone PUBLIC_HEALTH table --
-- from LZ.PUBLIC_HEALTH table --------------------
---------------------------------------------------

CREATE TABLE IF NOT EXISTS public."STG.PUBLIC_HEALTH"
(
    "Community Area" numeric,
    "Below Poverty Level" double precision,
    "Per Capita Income" numeric,
    "Unemployment" double precision
)

-- INSERT INTO statement
INSERT INTO public."STG.PUBLIC_HEALTH" (
    "Community Area",
    "Below Poverty Level",
    "Per Capita Income",
    "Unemployment"
)
SELECT 
    "Community Area",
    "Below Poverty Level",
    "Per Capita Income",
    "Unemployment"
FROM 	
    public."LZ.PUBLIC_HEALTH";

EOF
)

# Run the SQL commands
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$SQL_COMMANDS"