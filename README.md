Scripts folder contain shell scripts for data lading and integration

Either scripts can be executed one by one as below -
./landing_zone.sh - Loads csv data to landing zone
./staging_zone.sh - Loads data fromn ladning to staging zone with transformations
./summarised_zone.sh - Loads data from staging zone to summarised zone wfollowing joisn and aggregations
./summarised_zone_gcp.sh - move summarised zoen data to gcp postgresql instance

Or can be executed with one orchaestraion script as below -
./run_all.sh - For end to end data loading -> data pre-processing -> moving data to GCP 

