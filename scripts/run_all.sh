#!/bin/bash

# Define the script files
SCRIPTS=("landing_zone.sh" "staging_zone.sh" "summarised_zone.sh" "gcp_summarised_zone.sh")

# Loop through each script and execute it
for script in "${SCRIPTS[@]}"; do
    echo "Running $script..."
    if ./$script; then
        echo "$script executed successfully."
    else
        echo "Error executing $script. Exiting."
        exit 1
    fi
done

echo "All scripts executed successfully."