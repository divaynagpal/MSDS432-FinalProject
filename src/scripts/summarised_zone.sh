#!/bin/bash

# Database connection parameters
DB_NAME="ms432_project"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
DB_PASSWORD="your_database_password"

# SQL commands
SQL_COMMANDS=$(cat <<EOF
------------------------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL01" ----------------------------------
-- Send alerts to taxi drivers about the state of covid 19 in different zip codes --
-- This script Aggregates total trip count (Pickup and Dropoff) and weekly --------- 
-- positive cases for each zip code and week ---------------------------------------
-- To forecast severity - Average of last 3 week's "Total Trip Count" --------------
-- and "Weekly Positive Cases" are considered --------------------------------------
-- Tables used ---------------------------------------------------------------------
-- public."STG.TRIPS_DATA" ---------------------------------------------------------
-- public."STG.COVID_BY_ZIP" -------------------------------------------------------
-- public."LZ.BOUNDARIES_ZIP" ------------------------------------------------------
------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL01" AS
WITH ALL_WEEKS_ZIPS AS (
    SELECT DISTINCT 
        cbz."Week Number",
        z."ZIP"
    FROM
        public."STG.COVID_BY_ZIP" cbz,
        public."LZ.BOUNDARIES_ZIP" z
),
TRIP_COUNTS AS (
    SELECT 
        t."Week Number",
        t."Pickup Zip" AS "ZIP Code",
        COUNT(t."Trip ID") AS "Trip Count" 
    FROM 
        public."STG.TRIPS_DATA" t
    WHERE 
        t."Pickup Zip" IS NOT NULL
    GROUP BY
        t."Week Number",
        t."Pickup Zip"
    UNION 
    SELECT 
        t."Week Number",
        t."Dropoff Zip" AS "ZIP Code",
        COUNT(t."Trip ID") AS "Trip Count" 
    FROM 
        public."STG.TRIPS_DATA" t
    WHERE 
        t."Dropoff Zip" IS NOT NULL
    GROUP BY
        t."Week Number",
        t."Dropoff Zip"
),
COMBINED_TRIP_COUNTS AS (
    SELECT 
        "Week Number",
        "ZIP Code",
        SUM("Trip Count") AS "Total Trip Count"
    FROM 
        TRIP_COUNTS
    GROUP BY
        "Week Number",
        "ZIP Code"
)
SELECT
    cbz."Week Start",
    'WEEK-' || awz."Week Number" as "Week Number",
    awz."ZIP",
    coalesce(ctc."Total Trip Count", 0) AS "Total Trip Count",
    coalesce(cbz."Cases - Weekly", 0) AS "Weekly Positive Cases",
	'' as "Severity",
	'' as "Forecasted Severity"
FROM 
    ALL_WEEKS_ZIPS awz
    LEFT JOIN COMBINED_TRIP_COUNTS ctc ON awz."ZIP" = ctc."ZIP Code" AND awz."Week Number" = ctc."Week Number"
    LEFT JOIN public."STG.COVID_BY_ZIP" cbz ON awz."ZIP" = cbz."ZIP Code"::numeric AND awz."Week Number" = cbz."Week Number"
ORDER BY
    cbz."Week Start" DESC,
    awz."ZIP";
	
----------------------------------------------------------------------------
-- Update the Actual Severity Based on IQR 
-- Calculate IQR of Total Trip Count and Weekly Positive Cases
-- If Trip count is below Q1_trip_count rank it as 1 
-- If Trip count is between Q1_trip_count and Q3_trip_count rank it as 2
-- If Trip count is above Q3_trip_count rank it as 3 
-- Simliar for weekly postive cases, rank according to Q1_cases and Q3_cases
-- Add ranks of both trip count and weekly positive cases
-- If combined rank is 6, 5 - classify as 'High'
-- If combined rank is 4 - classify as 'Medium'
-- If combined rank is 3, 2 - classify as 'Low'
-----------------------------------------------------------------------------

WITH iqr_values AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "Total Trip Count") AS Q1_trip_count,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "Total Trip Count") AS Q3_trip_count,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "Weekly Positive Cases") AS Q1_cases,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "Weekly Positive Cases") AS Q3_cases
    FROM public."SMZ.RPTTBL01"
)
UPDATE public."SMZ.RPTTBL01" AS rpt
SET "Severity" = CASE
	WHEN "Total Trip Count" > iqr.Q3_trip_count
         AND "Weekly Positive Cases" > iqr.Q3_cases THEN 'High'
	WHEN "Total Trip Count" > iqr.Q3_trip_count
         AND "Weekly Positive Cases" BETWEEN iqr.Q1_cases AND iqr.Q3_cases THEN 'High' 
	WHEN "Total Trip Count" BETWEEN iqr.Q1_trip_count AND iqr.Q3_trip_count
		AND "Weekly Positive Cases" > iqr.Q3_cases THEN 'High'
    WHEN "Total Trip Count" BETWEEN iqr.Q1_trip_count AND iqr.Q3_trip_count
         AND "Weekly Positive Cases" BETWEEN iqr.Q1_cases AND iqr.Q3_cases THEN 'Medium'
	WHEN "Total Trip Count" > iqr.Q3_trip_count
         AND "Weekly Positive Cases" < iqr.Q1_cases THEN 'Medium'	
	WHEN "Total Trip Count" < iqr.Q1_trip_count
		AND "Weekly Positive Cases" > iqr.Q3_cases THEN 'Medium'	 
    ELSE 'Low'
END
FROM iqr_values iqr;

--------------------------------------------------------------------------
-- Update the Forecasted Severity Based on Last 3 Weeks Data
-- calculate last 3 week's average of trip count and weekly postive cases
-- Classify them as High, Low , Medium as per previous logic
----------------------------------------------------------------------------

WITH iqr_values AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "Total Trip Count") AS Q1_trip_count,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "Total Trip Count") AS Q3_trip_count,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "Weekly Positive Cases") AS Q1_cases,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "Weekly Positive Cases") AS Q3_cases
    FROM public."SMZ.RPTTBL01"
),
avg_last_3_weeks AS (
    SELECT
        "ZIP",
        "Week Number",
        AVG("Total Trip Count") OVER (
			PARTITION BY "ZIP" 
			ORDER BY "Week Start" DESC 
			ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
		) AS avg_trip_count,
        AVG("Weekly Positive Cases") OVER (
			PARTITION BY "ZIP" 
			ORDER BY "Week Start" DESC 
			ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
		) AS avg_cases
    FROM public."SMZ.RPTTBL01"
),
rpt_with_avg AS (
	SELECT 
		rpt.*,
		avg.avg_trip_count,
        avg.avg_cases
    FROM 
        public."SMZ.RPTTBL01" rpt
    LEFT JOIN 
        avg_last_3_weeks avg
    ON 
        rpt."ZIP" = avg."ZIP" AND rpt."Week Number" = avg."Week Number"
)
UPDATE public."SMZ.RPTTBL01" AS rpt
SET "Forecasted Severity" = CASE
	WHEN favg.avg_trip_count > iqr.Q3_trip_count
         AND favg.avg_cases > iqr.Q3_cases THEN 'High'
	WHEN favg.avg_trip_count > iqr.Q3_trip_count
         AND favg.avg_cases BETWEEN iqr.Q1_cases AND iqr.Q3_cases THEN 'High' 
	WHEN favg.avg_trip_count BETWEEN iqr.Q1_trip_count AND iqr.Q3_trip_count
		AND favg.avg_cases > iqr.Q3_cases THEN 'High'
    WHEN favg.avg_trip_count BETWEEN iqr.Q1_trip_count AND iqr.Q3_trip_count
         AND favg.avg_cases BETWEEN iqr.Q1_cases AND iqr.Q3_cases THEN 'Medium'
	WHEN favg.avg_trip_count > iqr.Q3_trip_count
         AND favg.avg_cases < iqr.Q1_cases THEN 'Medium'	
	WHEN favg.avg_trip_count < iqr.Q1_trip_count
		AND favg.avg_cases > iqr.Q3_cases THEN 'Medium'	 
    ELSE 'Low'
END
FROM rpt_with_avg favg, iqr_values iqr
WHERE rpt."ZIP" = favg."ZIP" AND rpt."Week Number" = favg."Week Number"; --12803 ( 1 sec)

----------------------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL02" --------------------------------
-- Purpose : A report containing Airport name where trip started, week start, ----
-- drop-off zip codes, number of trips to corresponding zip codes in that week, -- 
-- number of reported positive test cases in the zip code for that week ----------
-- Aggregates total trip count (From airport to zip codes) and -------------------
-- weekly positive cases for each zip code and week ------------------------------
-- Tables used : -----------------------------------------------------------------
-- public."STG.TRIPS_DATA" -------------------------------------------------------
-- public."STG.COVID_BY_ZIP" -----------------------------------------------------
-- public."LZ.BOUNDARIES_ZIP" ----------------------------------------------------
-- zip codes considered : (Midway - 60638), (O'Hare - 60656, 60666) --------------
----------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS 	public."SMZ.RPTTBL02" AS
WITH AIRPORT AS (
	SELECT 'Midway' AS "Airport"
    UNION ALL
    SELECT 'O''Hare' AS "Airport"
),
	ALL_WEEKS_ZIPS AS (
    SELECT DISTINCT
        cbz."Week Number",
		a."Airport",
        z."ZIP"
    FROM
        public."STG.COVID_BY_ZIP" cbz,
        public."LZ.BOUNDARIES_ZIP" z,
		AIRPORT a
),
AIRPORT_TRIP_COUNT AS (
SELECT
	"Week Number",
	CASE
		WHEN "Pickup Zip" = 60638 THEN 'Midway'
		WHEN "Pickup Zip" IN (60656, 60666) THEN 'O''Hare'
		END AS "From Airport",
	"Dropoff Zip" AS "To Zip Code",
	COUNT("Trip ID") AS "Total Trip Count from Airport to Zip Code"
	FROM
		public."STG.TRIPS_DATA"
	WHERE
		"Pickup Zip" IN (60638, 60656, 60666) AND "Dropoff Zip" IS NOT NULL
	GROUP BY 
		"Week Number",
		"From Airport",
		"Dropoff Zip"
	ORDER BY 
		"Week Number",
		"From Airport",
		"Dropoff Zip"
)
SELECT
	cbz."Week Start",
	'WEEK-' || awz."Week Number" as "Week Number",
	awz."Airport" AS "From Airport",
    awz."ZIP" AS "To Zip Code",
	coalesce(atc."Total Trip Count from Airport to Zip Code", 0) AS "Total Trip Count from Airport to Zip Code",
	coalesce(cbz."Cases - Weekly", 0) AS "Weekly Positive Cases"
FROM 
	ALL_WEEKS_ZIPS awz
	LEFT JOIN 
		AIRPORT_TRIP_COUNT atc 
	ON awz."Week Number" = atc."Week Number" 
		AND awz."Airport" = atc."From Airport"
		AND awz."ZIP" = atc."To Zip Code" 
	LEFT JOIN 
		public."STG.COVID_BY_ZIP" cbz 
	ON awz."ZIP" = cbz."ZIP Code"::numeric 
		AND awz."Week Number" = cbz."Week Number"
ORDER BY 
	cbz."Week Start" DESC,
    awz."Airport",
    awz."ZIP";

--------------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL03" ------------------------
-- Purpose : Daily report to track the number of taxi trips from/to the --
-- neighborhoods that have CCVI Category with value HIGH -----------------
-- Aggregates to and from trip count fper week each community area -------
-- having CCVI Catuegory as 'HIGH'----------------------------------------
-- Tables used : ---------------------------------------------------------
-- public."STG.TRIPS_DATA" -----------------------------------------------
-- public."STG.CCVI_BY_CA" -----------------------------------------------
--------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS 	public."SMZ.RPTTBL03" AS
WITH FROM_TRIP_COUNT AS
(
	SELECT
		"Week Number",
		"Week Start",
		"Pickup Community Area" AS "Community Area",
		COUNT("Trip ID") AS "From Trip Count"
	FROM 
		public."STG.TRIPS_DATA" 
	WHERE 
		"Pickup Community Area" IS NOT NULL AND "Pickup Community Area" <> 0
	GROUP BY 
		"Week Number",
		"Week Start",
		"Pickup Community Area" 
),
TO_TRIP_COUNT AS (
	SELECT
		"Week Number",
		"Week Start",
		"Dropoff Community Area" AS "Community Area",
		COUNT("Trip ID") AS "To Trip Count"
	FROM 
		public."STG.TRIPS_DATA" 
	WHERE 
		"Dropoff Community Area" IS NOT NULL AND "Dropoff Community Area" <> 0
	GROUP BY 
		"Week Number",
		"Week Start",
		"Dropoff Community Area" 	
),
FROM_TO_TRIP_COUNT AS (
	SELECT 
		COALESCE(ftc."Week Number", ttc."Week Number") AS "Week Number",
		COALESCE(ftc."Week Start", ttc."Week Start") AS "Week Start",
    	COALESCE(ftc."Community Area", ttc."Community Area") AS "Community Area",
    	COALESCE(ftc."From Trip Count", 0) AS "From Trip Count",
    	COALESCE(ttc."To Trip Count", 0) AS "To Trip Count"
	FROM
		FROM_TRIP_COUNT ftc
	FULL OUTER JOIN 
		TO_TRIP_COUNT ttc 
	ON 
		ftc."Week Start" = ttc."Week Start" 
		AND ftc."Community Area" = ttc."Community Area"
)
SELECT 
	'WEEK-' || fttc."Week Number" as "Week Number",
	fttc."Week Start",
	cbc."Community Area",
	fttc."From Trip Count" AS "Number of Trips From Community Area",
	fttc."To Trip Count" AS "Number of Trips To Community Area",
	cbc."CCVI Category"
FROM 
	public."STG.CCVI_BY_CA" cbc
	LEFT JOIN FROM_TO_TRIP_COUNT fttc 
	ON cbc."Community Area" = fttc."Community Area"
WHERE 
	UPPER(cbc."CCVI Category") = 'HIGH'		
ORDER BY 
	fttc."Week Start" DESC,
	cbc."Community Area";
	
-------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL04A" ----------------
-- Purpose : Weekly reports to forecast traffic -------------------
-- patterns utilizing the taxi trips for the different zip codes --
-- and different community areas  ---------------------------------
-- Aggregates to and from trip count per week for each zip code ---
-- Forecasted Total Trip Count is calculated based on Average ----- 
-- of last 3 week's Total Trip Count ------------------------------
-- Tables used : --------------------------------------------------
-- public."STG.TRIPS_DATA" ----------------------------------------
-- public."LZ.BOUNDARIES_ZIP" -------------------------------------
-------------------------------------------------------------------
	
CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL04A" AS
WITH ALL_WEEKS_ZIPS AS (
	SELECT DISTINCT 
		t."Week Number",
		t."Week Start",
		z."ZIP"
	FROM
		public."STG.TRIPS_DATA" t,
        public."LZ.BOUNDARIES_ZIP" z
),
TRIP_COUNTS AS (
	SELECT 
		t."Week Start",
		t."Pickup Zip" AS "ZIP Code",
		COUNT(t."Trip ID") AS "Trip Count" 
	FROM 
		public."STG.TRIPS_DATA" t
	WHERE 
    	t."Pickup Zip" IS NOT NULL
	GROUP BY
        t."Week Start",
        t."Pickup Zip"
	UNION 
	SELECT 
		t."Week Start",
		t."Dropoff Zip" AS "ZIP Code",
		COUNT(t."Trip ID") AS "Trip Count" 
	FROM 
		public."STG.TRIPS_DATA" t
	WHERE 
    	t."Dropoff Zip" IS NOT NULL
	GROUP BY
        t."Week Start",
        t."Dropoff Zip"
),
COMBINED_TRIP_COUNTS AS (
	SELECT 
		"Week Start",
		"ZIP Code",
		SUM("Trip Count") AS "Total Trip Count"
	FROM 
		TRIP_COUNTS
	GROUP BY
		"Week Start",
		"ZIP Code"
)	
SELECT 
	'WEEK-' || awz."Week Number" as "Week Number",
	awz."Week Start",
	awz."ZIP",
	COALESCE(ctc."Total Trip Count", 0) AS "Total Trip Count",
	0 as "Forecasted Trip Count"
FROM 
	ALL_WEEKS_ZIPS awz
	LEFT JOIN COMBINED_TRIP_COUNTS ctc 
	ON awz."Week Start" = ctc."Week Start" AND awz."ZIP" = ctc."ZIP Code"
ORDER BY 
	awz."Week Start" DESC,
	awz."ZIP";
	
----------------------------------	
-- Update Forecasted trip count	--
----------------------------------

WITH forecasted_trip_count AS (
SELECT 
	"Week Number",
	"Week Start",
	"ZIP",
	AVG("Total Trip Count") OVER (
		PARTITION BY "ZIP" 
		ORDER BY "Week Start" DESC 
		ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
	) AS avg_total_trip_count	
FROM 	
	public."SMZ.RPTTBL04A"
)
UPDATE public."SMZ.RPTTBL04A" as rpt
SET "Forecasted Trip Count" = COALESCE(ftc."avg_total_trip_count",0)
FROM 
	forecasted_trip_count ftc
WHERE rpt."Week Number" = ftc."Week Number" AND rpt."ZIP" = ftc."ZIP";

-------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL04B" ----------------
-- Purpose : Monthly reports to forecast traffic ------------------
-- patterns utilizing the taxi trips for the different zip codes --
-- and different community areas  ---------------------------------
-- Aggregates to and from trip count per month for each zip code --
-- Forecast total trip count based on the average trip count ------
-- of last 3 months and update the forecasted trip count column ---
-- Tables used : --------------------------------------------------
-- public."STG.TRIPS_DATA" ----------------------------------------
-- public."LZ.BOUNDARIES_ZIP" -------------------------------------
-------------------------------------------------------------------
	
CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL04B" AS
WITH ALL_WEEKS_ZIPS AS (
	SELECT DISTINCT 
		t."Month Number",
		z."ZIP"
	FROM
		public."STG.TRIPS_DATA" t,
        public."LZ.BOUNDARIES_ZIP" z
),
TRIP_COUNTS AS (
	SELECT 
		t."Month Number",
		t."Pickup Zip" AS "ZIP Code",
		COUNT(t."Trip ID") AS "Trip Count" 
	FROM 
		public."STG.TRIPS_DATA" t
	WHERE 
    	t."Pickup Zip" IS NOT NULL
	GROUP BY
        t."Month Number",
        t."Pickup Zip"
	UNION 
	SELECT 
		t."Month Number",
		t."Dropoff Zip" AS "ZIP Code",
		COUNT(t."Trip ID") AS "Trip Count" 
	FROM 
		public."STG.TRIPS_DATA" t
	WHERE 
    	t."Dropoff Zip" IS NOT NULL
	GROUP BY
        t."Month Number",
        t."Dropoff Zip"
),
COMBINED_TRIP_COUNTS AS (
	SELECT 
		"Month Number",
		"ZIP Code",
		SUM("Trip Count") AS "Total Trip Count"
	FROM 
		TRIP_COUNTS
	GROUP BY
		"Month Number",
		"ZIP Code"
)	
SELECT 
	awz."Month Number",
	awz."ZIP",
	COALESCE(ctc."Total Trip Count", 0) AS "Total Trip Count",
	0 AS "Forecasted Trip Count"
FROM 
	ALL_WEEKS_ZIPS awz
	LEFT JOIN COMBINED_TRIP_COUNTS ctc 
	ON awz."Month Number" = ctc."Month Number" AND awz."ZIP" = ctc."ZIP Code"
ORDER BY 
    CAST(SPLIT_PART(awz."Month Number", '-', 2) AS INTEGER) DESC, -- Year part
    CAST(SPLIT_PART(awz."Month Number", '-', 1) AS INTEGER) DESC,
	awz."ZIP";	 
	
WITH forecasted_trip_count AS (
SELECT 
	"Month Number",
	"ZIP",
	AVG("Total Trip Count") OVER (
		PARTITION BY "ZIP" 
		ORDER BY 
			CAST(SPLIT_PART("Month Number",'-',2) AS INTEGER) DESC, -- Year
			CAST(SPLIT_PART("Month Number",'-',1) AS INTEGER) DESC 	-- Month
		ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
	) AS avg_total_trip_count	
FROM 	
	public."SMZ.RPTTBL04B"
)
UPDATE public."SMZ.RPTTBL04B" as rpt
SET "Forecasted Trip Count" = COALESCE(ftc."avg_total_trip_count",0)
FROM 
	forecasted_trip_count ftc
WHERE rpt."Month Number" = ftc."Month Number" AND rpt."ZIP" = ftc."ZIP";

-------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL04C" -----------------
-- Purpose : Daily reports to forecast traffic --------------------
-- patterns utilizing the taxi trips for the different zip codes --
-- and different community areas  ---------------------------------
-- Aggregates to and from trip count per day for each zip code ----
-- Forecasted Total Trip Count is calculated based on Average ----- 
-- of last 3 day's Total Trip Count -------------------------------
-- Tables used : --------------------------------------------------
-- public."STG.TRIPS_DATA" ----------------------------------------
-- public."LZ.BOUNDARIES_ZIP" -------------------------------------
-------------------------------------------------------------------
	
CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL04C" AS
WITH ALL_WEEKS_ZIPS AS (
	SELECT DISTINCT 
		t."Trip Start Timestamp" AS "Date",
		z."ZIP"
	FROM
		public."STG.TRIPS_DATA" t,
        public."LZ.BOUNDARIES_ZIP" z
),
TRIP_COUNTS AS (
	SELECT 
		t."Trip Start Timestamp" AS "Date",
		t."Pickup Zip" AS "ZIP Code",
		COUNT(t."Trip ID") AS "Trip Count" 
	FROM 
		public."STG.TRIPS_DATA" t
	WHERE 
    	t."Pickup Zip" IS NOT NULL
	GROUP BY
        t."Trip Start Timestamp",
        t."Pickup Zip"
	UNION 
	SELECT 
		t."Trip Start Timestamp" AS "Date",
		t."Dropoff Zip" AS "ZIP Code",
		COUNT(t."Trip ID") AS "Trip Count" 
	FROM 
		public."STG.TRIPS_DATA" t
	WHERE 
    	t."Dropoff Zip" IS NOT NULL
	GROUP BY
        t."Trip Start Timestamp",
        t."Dropoff Zip"
),
COMBINED_TRIP_COUNTS AS (
	SELECT 
		"Date",
		"ZIP Code",
		SUM("Trip Count") AS "Total Trip Count"
	FROM 
		TRIP_COUNTS
	GROUP BY
		"Date",
		"ZIP Code"
)	
SELECT 
	awz."Date",
	awz."ZIP",
	COALESCE(ctc."Total Trip Count", 0) AS "Total Trip Count",
	0 AS "Forecasted Trip Count"
FROM 
	ALL_WEEKS_ZIPS awz
	LEFT JOIN COMBINED_TRIP_COUNTS ctc 
	ON awz."Date" = ctc."Date" AND awz."ZIP" = ctc."ZIP Code"
ORDER BY 
	awz."Date" DESC,
	awz."ZIP";	 
	
WITH forecasted_trip_count AS (
SELECT 
	"Date",
	"ZIP",
	AVG("Total Trip Count") OVER (
		PARTITION BY "ZIP" 
		ORDER BY "Date" DESC
		ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
	) AS avg_total_trip_count	
FROM 	
	public."SMZ.RPTTBL04C"
)
UPDATE public."SMZ.RPTTBL04C" as rpt
SET "Forecasted Trip Count" = COALESCE(ftc."avg_total_trip_count",0)
FROM 
	forecasted_trip_count ftc
WHERE rpt."Date" = ftc."Date" AND rpt."ZIP" = ftc."ZIP";

-------------------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL05" -----------------------------
-- Purpose : Reports to identify building data for top 5 ----------------------
-- neighborhoods with highest unemployment rate and poverty rate to waive ----- 
-- the fees for building permits in those neighborhoods -----------------------
-- This Script identifies top 5 community areas with highest unemployment & ---
-- poverty level and aggegates teh total fees unpaid for each community area --
-- Tables used : --------------------------------------------------------------
-- public."STG.PUBLIC_HEALTH" -------------------------------------------------
-- public."STG.BUILDING_PERMIT" -----------------------------------------------
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL05" AS
WITH WAIVED_CA AS (
SELECT 
	"Community Area",
	"Unemployment" + "Below Poverty Level" AS "Score"
	FROM 
		public."STG.PUBLIC_HEALTH"
	ORDER BY 
		"Score" DESC
	LIMIT 5		
)
SELECT 
	wc."Community Area",
	SUM(bp."SUBTOTAL_UNPAID") AS "Total fees to be Waived"
FROM	
	WAIVED_CA wc
	LEFT JOIN public."STG.BUILDING_PERMIT" bp ON wc."Community Area" = bp."COMMUNITY_AREA"
GROUP BY 
	wc."Community Area"	
ORDER BY	
	"Total fees to be Waived";
------------------------------------------------------------------------------
-- Create summmarised table public."SMZ.RPTTBL06" ----------------------------
-- Purpose : identifying businesses with applications having PERMIT_TYPE of --
-- PERMIT - NEW CONSTRUCTION in the zip code that has the lowest number ------
-- of PERMIT - NEW CONSTRUCTION  applications and PER CAPITA INCOME is -------
-- less than 30,000 for the planned construction site in order to offer ------ 
-- small businesses low interest loans of up to $250,000-----------------------
-- This Script identifies zip codes and permit IDs with least number of new ---
-- constructions PER CAPITA INCOME less than 30000 ---------------------------- 
-- Tables used : --------------------------------------------------------------
-- public."STG.PUBLIC_HEALTH" -------------------------------------------------
-- public."STG.BUILDING_PERMIT" -----------------------------------------------
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."SMZ.RPTTBL06" AS
WITH NEW_CONSTRUCTION_COUNT AS (
SELECT 
	"ZIP",
	COUNT("PERMIT_TYPE") AS "Number of PERMIT - NEW CONSTRUCTION"
FROM 
	public."STG.BUILDING_PERMIT"
WHERE 
	TRIM("PERMIT_TYPE") = 'PERMIT - NEW CONSTRUCTION'
GROUP BY 
	"ZIP"
),
MinPermitCount AS (
    SELECT 
        MIN("Number of PERMIT - NEW CONSTRUCTION") AS MinCount
    FROM 
        NEW_CONSTRUCTION_COUNT
),
ZIP_WITH_MIN_NEW_CONSTRUCTION_COUNT AS (
SELECT 
	ncc.*
FROM 	
	NEW_CONSTRUCTION_COUNT ncc
JOIN 	
	MinPermitCount mpc ON ncc."Number of PERMIT - NEW CONSTRUCTION" = mpc.MinCount
)
SELECT 
	bp."ZIP",
	bp."PERMIT#" AS "Permit ID - ELIGIBLE FOR LOAN"
FROM 
	public."STG.BUILDING_PERMIT" bp
INNER JOIN 
	public."STG.PUBLIC_HEALTH" ph
ON 	
	bp."COMMUNITY_AREA" = ph."Community Area"
INNER JOIN
	ZIP_WITH_MIN_NEW_CONSTRUCTION_COUNT ncc
ON
	bp."ZIP" = ncc."ZIP"
WHERE 
	TRIM(bp."PERMIT_TYPE") = 'PERMIT - NEW CONSTRUCTION'
	AND ph."Per Capita Income" < 30000;

EOF
)

# Run the SQL commands
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$SQL_COMMANDS"


# Export the data to a CSV file using psql
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL01\" TO 'RPTTBL01.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL02\" TO 'RPTTBL02.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL03\" TO 'RPTTBL03.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL04A\" TO 'RPTTBL04A.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL04B\" TO 'RPTTBL04B.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL04C\" TO 'RPTTBL04C.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL05\" TO 'RPTTBL05.csv' WITH CSV HEADER"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "\COPY public.\"SMZ.RPTTBL06\" TO 'RPTTBL06.csv' WITH CSV HEADER"