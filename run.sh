#!/bin/bash

# Parivesh - India Environmental Approvals Data Collection Pipeline
# This script runs the complete data collection pipeline for 
# a specified state or all states

# Indian States and Union Territories with LGD Codes
# Usage: ./run.sh <LGD_CODE>
# Example: ./run.sh 30 (for Goa)
# Example: ./run.sh (for all India)
#
# LGD Code	State Name 
# 35	Andaman And Nicobar Islands
# 28	Andhra Pradesh
# 12	Arunachal Pradesh
# 18	Assam
# 10	Bihar
# 4	Chandigarh
# 22	Chhattisgarh
# 7	Delhi
# 30	Goa
# 24	Gujarat
# 6	Haryana
# 2	Himachal Pradesh
# 1	Jammu And Kashmir
# 20	Jharkhand
# 29	Karnataka
# 32	Kerala
# 37	Ladakh
# 31	Lakshadweep
# 23	Madhya Pradesh
# 27	Maharashtra
# 14	Manipur
# 17	Meghalaya
# 15	Mizoram
# 13	Nagaland
# 21	Odisha
# 34	Puducherry
# 3	Punjab
# 8	Rajasthan
# 11	Sikkim
# 33	Tamil Nadu
# 36	Telangana
# 38	The Dadra And Nagar Haveli And Daman And Diu
# 16	Tripura
# 5	Uttarakhand
# 9	Uttar Pradesh
# 19	West Bengal

# Array of all Indian states and union territories LGD codes (in ascending order)
STATES=(
    1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 27 28 29 30 31 32 33 34 35 36 37 38
)

set -e  # Exit on any error

# Check if a state code is provided as argument
if [ $# -eq 1 ]; then
    # Run for specific state
    STATE="$1"
    echo "===================================="
    echo "Goa Environmental Approvals Pipeline"
    echo "===================================="
    echo "Running for state code: $STATE"
    echo

    # Step 1: Initialize
    echo "Step 1: Initializing data collection for $STATE..."
    bash 1_initialize.sh "$STATE"
    echo "✓ Initialization completed"
    echo

    # Step 2: Fetch data
    echo "Step 2: Fetching project data for $STATE..."
    bash 2_fetch.sh "$STATE"
    echo "✓ Data fetching completed"
    echo

    # Step 3: Parse
    echo "Step 3: Processing data and generating CSV for $STATE..."
    python3 3_parse.py "$STATE"
    echo "✓ Data processing completed"
    echo

    # Step 4: Generate GeoJSON
    echo "Step 4: Processing data and generating CSV for $STATE..."
    python3 4_make_shape.py $STATE
    echo "✓ Shape generation completed"
    echo

    # Step 5: Combine
    echo "Step 5: Combine shapes..."
    python3  5_combine_geojson.py
    echo "✓ Shapes combined"
    echo

    echo "===================================="
    echo "Pipeline completed successfully!"
    echo "Data saved to: geojson/Projects_$STATE.geojson"
    echo "===================================="

else
    # Run for all states
    echo "===================================="
    echo "Goa Environmental Approvals Pipeline"
    echo "===================================="
    echo "Running for all states (${#STATES[@]} states total)"
    echo

    for STATE in "${STATES[@]}"; do
        echo "===================================="
        echo "Processing state code: $STATE"
        echo "===================================="
        echo

        # Step 1: Initialize
        echo "Step 1: Initializing data collection for $STATE..."
        bash 1_initialize.sh "$STATE"
        echo "✓ Initialization completed for $STATE"
        echo

        # Step 2: Fetch data
        echo "Step 2: Fetching project data for $STATE..."
        bash 2_fetch.sh "$STATE"
        echo "✓ Data fetching completed for $STATE"
        echo

        # Step 3: Parse
        echo "Step 3: Processing data and generating CSV for $STATE..."
        python3 3_parse.py "$STATE"
        echo "✓ Data processing completed for $STATE"
        echo

        # Step 4: Generate GeoJSON
        echo "Step 4: Processing data and generating CSV for $STATE..."
        python3 4_make_shape.py $STATE
        echo "✓ Shape generation completed for $STATE"
        echo

        echo "Completed processing for state $STATE"
        echo "Data saved to: geojson/Projects_$STATE.geojson"
        echo
    done

    # Step 5: Combine (only once after all states are processed)
    echo "Step 5: Combine shapes for all states..."
    python3  5_combine_geojson.py
    echo "✓ Shapes combined for all states"
    echo

    echo "===================================="
    echo "Pipeline completed successfully for all states!"
    echo "===================================="
fi