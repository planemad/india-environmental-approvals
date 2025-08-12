#!/bin/bash

STATE=${1:-""}

# Create output directory with state suffix if filtering by state
if [ -n "$STATE" ]; then
  OUTPUT_DIR="raw/search_${STATE}"
  echo "Fetching data for state: $STATE"
else
  OUTPUT_DIR="raw/search"
  echo "Fetching data for all states"
fi

mkdir -p "$OUTPUT_DIR"

for clearance in {1..4};
do
  OUTPUT_FILE="$OUTPUT_DIR/${clearance}.json"
  API_URL="https://parivesh.nic.in/parivesh_api/trackYourProposal/advanceSearchData?majorClearanceType=${clearance}&state=${STATE}&sector=&proposalStatus=&proposalType=&issuingAuthority=&activityId=&category=&startDate=&endDate=&areaMin=&areaMax=&text="
  
  echo "Fetching clearance type $clearance for state '$STATE'..."
  curl "$API_URL" > "$OUTPUT_FILE"
  
  # Check if the file was created and has content
  if [ -s "$OUTPUT_FILE" ]; then
    echo "Successfully fetched data for clearance type $clearance"
  else
    echo "Warning: No data or empty response for clearance type $clearance"
  fi
  
done

echo "Data fetching completed. Files saved to: $OUTPUT_DIR"
