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
    # Quick check if it's valid JSON and has meaningful content in one jq call
    if jq -e 'if type == "object" then (keys | length > 0) else true end' "$file" >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

# Function to build a lookup of existing valid files for a clearance type
build_existing_files_lookup() {
  local clearance_dir="$1"
  local existing_files=""
  
  if [ -d "$clearance_dir" ]; then
    # Use find to get all .json files and check them in batch
    for file in "$clearance_dir"/*.json; do
      if [ -f "$file" ] && [ -s "$file" ]; then
        # Extract just the filename without extension
        local basename=$(basename "$file" .json)
        existing_files="$existing_files$basename "
      fi
    done
  fi
  echo "$existing_files"
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
downloaded_new=0
failed_downloads=0
skipped_batch=0

# First pass: count total proposals across all clearance types
echo "Counting total proposals..."
total_proposals_all=0
for clearance in {1..4}; do
  SEARCH_FILE="$SEARCH_DIR/${clearance}.json"
  if [ -s "$SEARCH_FILE" ]; then
    clearance_count=$(cat "$SEARCH_FILE" | jq '[.data[] | select(.id != null)] | length' 2>/dev/null || echo "0")
    total_proposals_all=$((total_proposals_all + clearance_count))
  fi
done
echo "Total proposals to process: $total_proposals_all"
echo ""

processed_count=0

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
  
  # Build lookup of existing files for this clearance type
  echo "  Building lookup of existing files..."
  existing_files_lookup=$(build_existing_files_lookup "$CAF_DIR/${clearance}")
  
  # Extract all proposal IDs first to avoid repeated jq calls
  echo "  Extracting proposal IDs..."
  proposal_ids=$(cat "$SEARCH_FILE" | jq -r '.data[] | select(.id != null) | .id' 2>/dev/null || echo "")
  
  if [ -z "$proposal_ids" ]; then
    echo "  No valid proposal IDs found in $SEARCH_FILE"
    continue
  fi
  
  skipped_batch=0
  last_progress_update=0
  
  for proposal_id in $proposal_ids;
  do
    total_proposals=$((total_proposals + 1))
    processed_count=$((processed_count + 1))
    remaining=$((total_proposals_all - processed_count))
    
    # Quick lookup check - much faster than file system calls
    if [[ " $existing_files_lookup " == *" $proposal_id "* ]]; then
      # File exists, skip it completely
      skipped_existing=$((skipped_existing + 1))
      skipped_batch=$((skipped_batch + 1))
      
      # Only print progress every 50 skipped files to reduce output noise
      if [ $((skipped_batch % 50)) -eq 0 ] || [ $((processed_count - last_progress_update)) -ge 100 ]; then
        echo "[$processed_count/$total_proposals_all, $remaining remaining] Skipped $skipped_batch existing files (last: $proposal_id)"
        last_progress_update=$processed_count
      fi
    else
      # Reset batch counter when we encounter a new file
      if [ $skipped_batch -gt 0 ]; then
        echo "[$processed_count/$total_proposals_all, $remaining remaining] Finished skipping $skipped_batch existing files"
        skipped_batch=0
      fi
      
      OUTPUT_FILE="$CAF_DIR/${clearance}/${proposal_id}.json"
      
      # File doesn't exist, fetch it
      echo "[$processed_count/$total_proposals_all, $remaining remaining] Fetching new proposal: $proposal_id"
      if safe_download "https://parivesh.nic.in/parivesh_api/proponentApplicant/getCafDataByProposalNo?proposal_id=${proposal_id}" "$OUTPUT_FILE"; then
        downloaded_new=$((downloaded_new + 1))
        # Add to lookup for future iterations (though this clearance is almost done)
        existing_files_lookup="$existing_files_lookup$proposal_id "
      else
        failed_downloads=$((failed_downloads + 1))
      fi
      sleep $(echo "scale=3; $RANDOM / 32767 * 0.5" | bc)
    fi
  done
  
  # Print final batch summary if we ended on skipped files
  if [ $skipped_batch -gt 0 ]; then
    echo "  Completed clearance $clearance: skipped $skipped_batch existing files in final batch"
  fi
done

echo "Data fetching completed. Files saved to: $CAF_DIR"
echo "Summary:"
echo "  Total proposals processed: $total_proposals"
echo "  Skipped (existing): $skipped_existing"
echo "  Downloaded new: $downloaded_new"
echo "  Failed downloads: $failed_downloads"

if [ $failed_downloads -gt 0 ]; then
  echo "Warning: $failed_downloads downloads failed. You may want to re-run this script."
fi
