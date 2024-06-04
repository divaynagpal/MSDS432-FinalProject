#!/bin/bash

# Database connection parameters
DB_NAME="ms432_project"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
DB_PASSWORD="your_database_password"

# SQL commands
SQL_COMMANDS=$(cat <<EOF
---------------------------------------------------------------
-- Enable POSTGIS extension to store and handle spatial data --
---------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis; 

--------------------------------------------------------
-- Create landing zone TAXI_TRIPS table and load data --
--------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.TAXI_TRIPS"
(
    "Trip ID" text,
    "Taxi ID" text,
    "Trip START Timestamp" date,
    "Trip End Timestamp" date,
    "Trip Seconds" numeric,
    "Trip Miles" numeric,
    "Pickup Census Tract" numeric,
    "Dropoff Census Tract" numeric,
    "Pickup Community Area" numeric,
    "Dropoff Community Area" numeric,
    "Fare" numeric,
    "Tips" numeric,
    "Tolls" numeric,
    "Extras" numeric,
    "Trip Total" numeric,
    "Payment Type" text,
    "Company" text,
    "Pickup Centroid Latitude" numeric,
    "Pickup Centroid Longitude" numeric,
    "Pickup Centroid Location" GEOMETRY(Point, 4326),
    "Dropoff Centroid Latitude" numeric,
    "Dropoff Centroid Longitude" numeric,
    "Dropoff Centroid Location" GEOMETRY(Point, 4326),
    "Community Areas" numeric
);

-- Derive week start based on "Trip Start Timestamp" - this is to match with covid dataset
ALTER TABLE public."LZ.TAXI_TRIPS"
ADD COLUMN "Week Start" date GENERATED ALWAYS AS 
(("Trip START Timestamp"::date - INTERVAL '1 day' * EXTRACT(DOW FROM "Trip START Timestamp")::integer)::date) STORED;

-- Derive week start as per ISO standards  - this is to match the week start and end with covid dataset
ALTER TABLE public."LZ.TAXI_TRIPS"
ADD COLUMN "Week Start ISO" date GENERATED ALWAYS AS 
(("Trip START Timestamp"::date - INTERVAL '1 day' * EXTRACT(DOW FROM "Trip START Timestamp")::integer + INTERVAL '1 day')::date) STORED;

-- Import data 
COPY public."LZ.TAXI_TRIPS" 
FROM '/Users/supriyajadhav/Downloads/Taxi_Trips.csv' 
DELIMITER ',' 
CSV HEADER; 
	
------------------------------------------------------------
-- Create Landing zone BOUNDARIES_ZIP table and load data -- 
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.BOUNDARIES_ZIP"
(
    "the_geom" GEOMETRY(MULTIPOLYGON, 4326),
    "OBJECTID" numeric,
    "ZIP" numeric,
    "SHAPE_AREA" DOUBLE PRECISION,
    "SHAPE_LEN" DOUBLE PRECISION
);
	
COPY public."LZ.BOUNDARIES_ZIP" 
FROM '/Users/supriyajadhav/Documents/MS432- Data Engineering/Project/Sample Data/Zip_Codes.csv' 
DELIMITER ',' 
CSV HEADER;

----------------------------------------------------------
-- Create Landing Zone COVID_BY_ZIP table and load data --
----------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.COVID_BY_ZIP"
(
	"ZIP Code" text,
	"Week Number" numeric,
	"Week Start" date,
	"Week End" date,
	"Cases - Weekly"  numeric,
 	"Cases - Cumulative"  numeric,
	"Case Rate - Weekly"  numeric,
 	"Case Rate - Cumulative"  numeric,
	"Tests - Weekly"  numeric,
	"Tests - Cumulative"  numeric,
 	"Test Rate - Weekly"  DOUBLE PRECISION,
	"Test Rate - Cumulative"  DOUBLE PRECISION,
	"Percent Tested Positive - Weekly"  text,
	"Percent Tested Positive - Cumulative"  text,
	"Deaths - Weekly"  numeric,
	"Deaths - Cumulative"  numeric,
	"Death Rate - Weekly"  numeric,
	"Death Rate - Cumulative"  numeric,
	"Population"  numeric,
	"Row ID"  text,
	"ZIP Code Location"  GEOMETRY(Point, 4326)
);

ALTER TABLE public."LZ.COVID_BY_ZIP"
ADD COLUMN "Week Start ISO" date GENERATED ALWAYS AS 
(("Week Start"::date + INTERVAL '1 day')::date) STORED;

COPY public."LZ.COVID_BY_ZIP" 
FROM '/Users/supriyajadhav/Downloads/COVID-19_Cases__Tests__and_Deaths_by_ZIP_Code_-_Historical_20240526.csv' 
DELIMITER ',' 
CSV HEADER; 

--------------------------------------------------------
-- Create Landing Zone CCVI_BY_CA table and load data --
--------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.CCVI_BY_CA"
(
	"Geography Type" text,
	"Community Area or ZIP Code" text,
	"Community Area Name" text,
	"CCVI Score" numeric,
	"CCVI Category" text,
    "Rank - Socioeconomic Status" numeric,
    "Rank - Household Composition and Disability" numeric,
    "Rank - Adults with no PCP" numeric,
    "Rank - Cumulative Mobility Ratio" numeric,
    "Rank - Frontline Essential Workers" numeric,
    "Rank - Age 65+" numeric,
    "Rank - Comorbid Conditions" numeric,
    "Rank - COVID-19 Incidence Rate" numeric,
    "Rank - COVID-19 Hospital Admission Rate" numeric,
    "Rank - COVID-19 Crude Mortality Rate" numeric,
    "Location" GEOMETRY(POINT, 4326)
);

COPY public."LZ.CCVI_BY_CA" 
FROM '/Users/supriyajadhav/Documents/MS432- Data Engineering/Project/Sample Data/CCVI_by_CA.csv' 
DELIMITER ',' 
CSV HEADER;

--------------------------------------------------------
-- Create Landing Zone PUBLIC_HEALTH table and load data --
--------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.PUBLIC_HEALTH"
(
	"Community Area" numeric,
	"Community Area Name" text,
	"Birth Rate" numeric,
	"General Fertility Rate" numeric,
	"Low Birth Weight" numeric,
	"Prenatal Care Beginning in First Trimester" numeric,
	"Preterm Births" numeric,
	"Teen Birth Rate" numeric,
	"Assault (Homicide)" numeric,
	"Breast cancer in females" numeric,
	"Cancer (All Sites)" numeric,
	"Colorectal Cancer" numeric,
	"Diabetes-related" numeric,
	"Firearm-related" numeric,
	"Infant Mortality Rate" numeric,
	"Lung Cancer" numeric,
	"Prostate Cancer in Males" numeric,
	"Stroke (Cerebrovascular Disease)" numeric,
	"Childhood Blood Lead Level Screening" numeric,
	"Childhood Lead Poisoning" numeric,
	"Gonorrhea in Females" numeric,
	"Gonorrhea in Males" text,
	"Tuberculosis" numeric,
	"Below Poverty Level" numeric,
	"Crowded Housing" numeric,
	"Dependency" numeric,
	"No High School Diploma" numeric,
	"Per Capita Income" numeric,
	"Unemployment" numeric
);
	
COPY public."LZ.PUBLIC_HEALTH" 
FROM '/Users/supriyajadhav/Documents/MS432- Data Engineering/Project/Sample Data/Public_Health_Statistics_Historic.csv' 
DELIMITER ',' 
CSV HEADER;	

--------------------------------------------------------
-- Create Landing Zone PUBLIC_HEALTH table and load data --
--------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.BUILDING_PERMIT"
(
"ID" text,
"PERMIT#" text,
"PERMIT_STATUS" text,
"PERMIT_MILESTONE" text,
"PERMIT_TYPE" text,
"REVIEW_TYPE" text,
"APPLICATION_START_DATE" date,
"ISSUE_DATE" date,
"PROCESSING_TIME" numeric,
"STREET_NUMBER" numeric,
"STREET_DIRECTION"  text,
"STREET_NAME" text,
"WORK_TYPE" text,
"WORK_DESCRIPTION" text,
"BUILDING_FEE_PAID" text,
"ZONING_FEE_PAID" text,
"OTHER_FEE_PAID" text,
"SUBTOTAL_PAID" text,
"BUILDING_FEE_UNPAID" text,
"ZONING_FEE_UNPAID" text,
"OTHER_FEE_UNPAID" text,
"SUBTOTAL_UNPAID" text,
"BUILDING_FEE_WAIVED" text,
"BUILDING_FEE_SUBTOTAL" text,
"ZONING_FEE_SUBTOTAL" text,
"OTHER_FEE_SUBTOTAL" text,
"ZONING_FEE_WAIVED" text,
"OTHER_FEE_WAIVED" text,
"SUBTOTAL_WAIVED" text,
"TOTAL_FEE" text,
"CONTACT_1_TYPE" text,
"CONTACT_1_NAME" text,
"CONTACT_1_CITY" text,
"CONTACT_1_STATE" text,
"CONTACT_1_ZIPCODE" text,
"CONTACT_2_TYPE" text,
"CONTACT_2_NAME" text,
"CONTACT_2_CITY" text,
"CONTACT_2_STATE" text,
"CONTACT_2_ZIPCODE" text,
"CONTACT_3_TYPE" text,
"CONTACT_3_NAME" text,
"CONTACT_3_CITY" text,
"CONTACT_3_STATE" text,
"CONTACT_3_ZIPCODE" text,
"CONTACT_4_TYPE" text,
"CONTACT_4_NAME" text,
"CONTACT_4_CITY" text,
"CONTACT_4_STATE" text,
"CONTACT_4_ZIPCODE" text,
"CONTACT_5_TYPE" text,
"CONTACT_5_NAME" text,
"CONTACT_5_CITY" text,
"CONTACT_5_STATE" text,
"CONTACT_5_ZIPCODE" text,
"CONTACT_6_TYPE" text,
"CONTACT_6_NAME" text,
"CONTACT_6_CITY" text,
"CONTACT_6_STATE" text,
"CONTACT_6_ZIPCODE" text,
"CONTACT_7_TYPE" text,
"CONTACT_7_NAME" text,
"CONTACT_7_CITY" text,
"CONTACT_7_STATE" text,
"CONTACT_7_ZIPCODE" text,
"CONTACT_8_TYPE" text,
"CONTACT_8_NAME" text,
"CONTACT_8_CITY" text,
"CONTACT_8_STATE" text,
"CONTACT_8_ZIPCODE" text,
"CONTACT_9_TYPE" text,
"CONTACT_9_NAME" text,
"CONTACT_9_CITY" text,
"CONTACT_9_STATE" text,
"CONTACT_9_ZIPCODE" text,
"CONTACT_10_TYPE" text,
"CONTACT_10_NAME" text,
"CONTACT_10_CITY" text,
"CONTACT_10_STATE" text,
"CONTACT_10_ZIPCODE" text,
"CONTACT_11_TYPE" text,
"CONTACT_11_NAME" text,
"CONTACT_11_CITY" text,
"CONTACT_11_STATE" text,
"CONTACT_11_ZIPCODE" text,
"CONTACT_12_TYPE" text,
"CONTACT_12_NAME" text,
"CONTACT_12_CITY" text,
"CONTACT_12_STATE" text,
"CONTACT_12_ZIPCODE" text,
"CONTACT_13_TYPE" text,
"CONTACT_13_NAME" text,
"CONTACT_13_CITY" text,
"CONTACT_13_STATE" text,
"CONTACT_13_ZIPCODE" text,
"CONTACT_14_TYPE" text,
"CONTACT_14_NAME" text,
"CONTACT_14_CITY" text,
"CONTACT_14_STATE" text,
"CONTACT_14_ZIPCODE" text,
"CONTACT_15_TYPE" text,
"CONTACT_15_NAME" text,
"CONTACT_15_CITY" text,
"CONTACT_15_STATE" text,
"CONTACT_15_ZIPCODE" text,
"REPORTED_COST" text,
"PIN_LIST" text,
"PIN1" text,
"PIN2" text,
"PIN3" text,
"PIN4" text,
"PIN5" text,
"PIN6" text,
"PIN7" text,
"PIN8" text,
"PIN9" text,
"PIN10" text,
"COMMUNITY_AREA" numeric,
"CENSUS_TRACT" numeric,
"WARD" numeric,
"XCOORDINATE" numeric,
"YCOORDINATE" numeric,
"LATITUDE" numeric,
"LONGITUDE" numeric,
"LOCATION" GEOMETRY(POINT,4326)
);

COPY public."LZ.BUILDING_PERMIT" 
FROM '/Users/supriyajadhav/Downloads/Building_Permits_20240512.csv' 
DELIMITER ',' 
CSV HEADER;

--------------------------------------------------------
-- Create Landing Zone COMM_AREA table and load data --
--------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."LZ.COMM_AREA"
(
	"the_geom" GEOMETRY(MULTIPOLYGON, 4326),
	"PERIMETER" numeric,
	"AREA" numeric,
	"COMAREA_" numeric,
	"COMAREA_ID" numeric,
	"AREA_NUMBE" numeric,
	"COMMUNITY" text,
	"AREA_NUM_1" numeric,
	"SHAPE_AREA" numeric,
	"SHAPE_LEN" numeric
);
	
copy public."LZ.COMM_AREA"
FROM '/Users/supriyajadhav/Documents/MS432- Data Engineering/Project/Sample Data/CommAreas.csv' 
DELIMITER ',' 
CSV HEADER;	

----------------------------
-- Create Indices ----------
----------------------------

-- Spatial indexes for geometry columns
CREATE INDEX idx_pickup_centroid_location
ON public."LZ.TAXI_TRIPS"
USING GIST ("Pickup Centroid Location");

CREATE INDEX idx_dropoff_centroid_location
ON public."LZ.TAXI_TRIPS"
USING GIST ("Dropoff Centroid Location");

CREATE INDEX idx_bz_the_geom
ON public."LZ.BOUNDARIES_ZIP"
USING GIST ("the_geom");

CREATE INDEX idx_ca_the_geom
ON public."LZ.COMM_AREA"
USING GIST ("the_geom");

-- Indexes on community area columns
CREATE INDEX idx_pickup_community_area
ON public."LZ.TAXI_TRIPS" ("Pickup Community Area");

CREATE INDEX idx_dropoff_community_area
ON public."LZ.TAXI_TRIPS" ("Dropoff Community Area");

EOF
)

# Run the SQL commands
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$SQL_COMMANDS"