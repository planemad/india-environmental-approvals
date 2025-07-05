#!/bin/bash

# State parameter - should match the one used in initialize.sh
STATE=${1:-""}

IFS=$'\n';

# Determine search directory based on state parameter
if [ -n "$STATE" ]; then
  SEARCH_DIR="raw/search_${STATE}"
  CAF_DIR="raw/caf_${STATE}"
  echo "Processing data for state: $STATE"
else
  SEARCH_DIR="raw/search"
  CAF_DIR="raw/caf"
  echo "Processing data for all states"
fi

# Check if search directory exists
if [ ! -d "$SEARCH_DIR" ]; then
  echo "Error: Search directory $SEARCH_DIR not found. Please run initialize.sh first."
  exit 1
fi

mkdir -p "$CAF_DIR"

# Function to check if a file is a valid JSON with actual data
is_valid_json_file() {
  local file="$1"
  # Check if file exists, is not empty, and contains valid JSON with data
  if [ -f "$file" ] && [ -s "$file" ]; then
    # Check if it's valid JSON and has more than just empty structure
    if jq -e . "$file" >/dev/null 2>&1; then
      # Check if it has meaningful content (not just {} or {"data":null} etc)
      local has_data=$(jq -r 'if type == "object" then (keys | length > 0) else true end' "$file" 2>/dev/null)
      if [ "$has_data" = "true" ]; then
        return 0
      fi
    fi
  fi
  return 1
}

# Function to safely download a file
safe_download() {
  local url="$1"
  local output_file="$2"
  local temp_file="${output_file}.tmp"
  
  # Download to temporary file first
  if curl -X POST --fail --silent --show-error "$url" -o "$temp_file"; then
    # Check if the downloaded file is valid JSON
    if jq -e . "$temp_file" >/dev/null 2>&1; then
      # Move temp file to final location
      mv "$temp_file" "$output_file"
      return 0
    else
      echo "Warning: Downloaded file is not valid JSON, removing temp file"
      rm -f "$temp_file"
      return 1
    fi
  else
    echo "Error: Failed to download from $url"
    rm -f "$temp_file"
    return 1
  fi
}

# Counters for progress tracking
total_proposals=0
skipped_existing=0
skipped_uptodate=0
downloaded_new=0
downloaded_updated=0
failed_downloads=0

for clearance in {1..4};
do
  SEARCH_FILE="$SEARCH_DIR/${clearance}.json"
  
  # Check if the search file exists and has content
  if [ ! -s "$SEARCH_FILE" ]; then
    echo "Warning: No data file found at $SEARCH_FILE, skipping clearance type $clearance"
    continue
  fi
  
  mkdir -p "$CAF_DIR/${clearance}"
  
  echo "Processing clearance type $clearance..."

  for proposal in $(cat "$SEARCH_FILE" | jq -c '.data[]');
  do
    proposal_id=$(echo "$proposal" | jq -r '.id')
    
    if [ "$proposal_id" = "null" ]; then
      continue
    fi

    total_proposals=$((total_proposals + 1))
    OUTPUT_FILE="$CAF_DIR/${clearance}/${proposal_id}.json"
    
    # Check if we have a valid existing file
    if is_valid_json_file "$OUTPUT_FILE"; then
      # File exists and is valid, check if we need to update it
      proposal_updated_on=$(echo "$proposal" | jq -r '.app_updated_on')
      
      if [[ "$proposal_updated_on" != "null" ]]; then
        # Convert date to timestamp - handle both macOS and Linux
        if [[ "$OSTYPE" == "darwin"* ]]; then
          # macOS - remove microseconds and use BSD date
          proposal_updated_on_clean=$(echo "$proposal_updated_on" | cut -d'.' -f1)
          proposal_updated_on_ts=$(date -j -f "%Y-%m-%d %H:%M:%S" "$proposal_updated_on_clean" +%s 2>/dev/null || echo "0")
          file_date=$(stat -f %m "$OUTPUT_FILE" 2>/dev/null || echo "0")
        else
          # Linux - use GNU date
          proposal_updated_on_ts=$(date -d "$proposal_updated_on" +%s 2>/dev/null || echo "0")
          file_date=$(date +%s -r "$OUTPUT_FILE" 2>/dev/null || echo "0")
        fi

        if [ "$file_date" -gt 0 ] && [ "$proposal_updated_on_ts" -gt 0 ] && [ $file_date -lt $proposal_updated_on_ts ]; then
          echo "Refetching $proposal_id due to application update since last fetch (updated: $proposal_updated_on)"
          if safe_download "https://parivesh.nic.in/parivesh_api/proponentApplicant/getCafDataByProposalNo?proposal_id=${proposal_id}" "$OUTPUT_FILE"; then
            downloaded_updated=$((downloaded_updated + 1))
          else
            failed_downloads=$((failed_downloads + 1))
          fi
          sleep 15
        else
          skipped_uptodate=$((skipped_uptodate + 1))
        fi
      else
        skipped_uptodate=$((skipped_uptodate + 1))
      fi
    else
      # File doesn't exist or is invalid, fetch it
      echo "Fetching new proposal: $proposal_id"
      if safe_download "https://parivesh.nic.in/parivesh_api/proponentApplicant/getCafDataByProposalNo?proposal_id=${proposal_id}" "$OUTPUT_FILE"; then
        downloaded_new=$((downloaded_new + 1))
      else
        failed_downloads=$((failed_downloads + 1))
      fi
      sleep 1
    fi
  done
done

echo "Data fetching completed. Files saved to: $CAF_DIR"
echo "Summary:"
echo "  Total proposals processed: $total_proposals"
echo "  Skipped (existing up-to-date): $skipped_uptodate"
echo "  Downloaded new: $downloaded_new"
echo "  Downloaded updated: $downloaded_updated"
echo "  Failed downloads: $failed_downloads"

if [ $failed_downloads -gt 0 ]; then
  echo "Warning: $failed_downloads downloads failed. You may want to re-run this script."
fi
