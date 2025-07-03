#!/bin/bash

# Indian States and Union Territories with LGD Codes
# Usage: ./initialize.sh <LGD_CODE>
# Example: ./initialize.sh 30 (for Goa)
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

# State parameter - default to all states if not specified
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
  
  sleep 60
done

echo "Data fetching completed. Files saved to: $OUTPUT_DIR"
