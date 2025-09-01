#!/bin/bash

# Parivesh - India Environmental Approvals Data Collection Pipeline
# This script runs the complete data collection pipeline for 
# a specified state or all states

# Indian States and Union Territories with LGD Codes
# Usage: ./run.sh <LGD_CODE>
# Example: ./run.sh 30 (for Goa)
# Example: ./run.sh (for all India)


# Function to get state name from state code
get_state_name() {
    local state_code="$1"
    case "$state_code" in
        1) echo "Jammu And Kashmir" ;;
        2) echo "Himachal Pradesh" ;;
        3) echo "Punjab" ;;
        4) echo "Chandigarh" ;;
        5) echo "Uttarakhand" ;;
        6) echo "Haryana" ;;
        7) echo "Delhi" ;;
        8) echo "Rajasthan" ;;
        9) echo "Uttar Pradesh" ;;
        10) echo "Bihar" ;;
        11) echo "Sikkim" ;;
        12) echo "Arunachal Pradesh" ;;
        13) echo "Nagaland" ;;
        14) echo "Manipur" ;;
        15) echo "Mizoram" ;;
        16) echo "Tripura" ;;
        17) echo "Meghalaya" ;;
        18) echo "Assam" ;;
        19) echo "West Bengal" ;;
        20) echo "Jharkhand" ;;
        21) echo "Odisha" ;;
        22) echo "Chhattisgarh" ;;
        23) echo "Madhya Pradesh" ;;
        24) echo "Gujarat" ;;
        27) echo "Maharashtra" ;;
        28) echo "Andhra Pradesh" ;;
        29) echo "Karnataka" ;;
        30) echo "Goa" ;;
        31) echo "Lakshadweep" ;;
        32) echo "Kerala" ;;
        33) echo "Tamil Nadu" ;;
        34) echo "Puducherry" ;;
        35) echo "Andaman And Nicobar Islands" ;;
        36) echo "Telangana" ;;
        37) echo "Ladakh" ;;
        38) echo "The Dadra And Nagar Haveli And Daman And Diu" ;;
        *) echo "Unknown State" ;;
    esac
}

# Array of all Indian states and union territories LGD codes (in ascending order)
STATES=(
    1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 27 28 29 30 31 32 33 34 35 36 37 38
)

set -e  # Exit on any error

# Check if a state code is provided as argument
if [ $# -eq 1 ]; then
    # Run for specific state
    STATE="$1"
    STATENAME=$(get_state_name "$STATE")
    
    echo "===================================="
    echo "India Environmental Approvals Data"
    echo "===================================="
    echo "Running for state code: $STATE"
    echo "State Name: $STATENAME"
    echo

    # Step 1: Initialize
    echo "Step 1: Initializing data collection for $STATE ($STATENAME)..."
    bash 1_initialize.sh "$STATE"
    echo "✓ Initialization completed"
    echo

    # Step 2: Fetch data
    echo "Step 2: Fetching project data for $STATE ($STATENAME)..."
    bash 2_fetch.sh "$STATE"
    echo "✓ Data fetching completed"
    echo

    # Step 3: Parse
    echo "Step 3: Processing data and generating CSV for $STATE ($STATENAME)..."
    python3 3_parse.py "$STATE"
    echo "✓ Data processing completed"
    echo

    # Step 4: Generate GeoJSON
    echo "Step 4: Processing data and generating CSV for $STATE ($STATENAME)..."
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
        STATENAME=$(get_state_name "$STATE")
        
        echo "===================================="
        echo "Processing state code: $STATE"
        echo "State Name: $STATENAME"
        echo "===================================="
        echo

        # Step 1: Initialize
        echo "Step 1: Initializing data collection for $STATE ($STATENAME)..."
        bash 1_initialize.sh "$STATE"
        echo "✓ Initialization completed for $STATE ($STATENAME)"
        echo

        # Step 2: Fetch data
        echo "Step 2: Fetching project data for $STATE ($STATENAME)..."
        bash 2_fetch.sh "$STATE"
        echo "✓ Data fetching completed for $STATE ($STATENAME)"
        echo

        # Step 3: Parse
        echo "Step 3: Processing data and generating CSV for $STATE ($STATENAME)..."
        python3 3_parse.py "$STATE"
        echo "✓ Data processing completed for $STATE ($STATENAME)"
        echo

        # Step 4: Generate GeoJSON
        echo "Step 4: Processing data and generating CSV for $STATE ($STATENAME)..."
        python3 4_make_shape.py $STATE
        echo "✓ Shape generation completed for $STATE ($STATENAME)"
        echo

        echo "Completed processing for state $STATE ($STATENAME)"
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