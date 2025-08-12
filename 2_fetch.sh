#!/bin/bash

# State parameter - should match the one used in initialize.sh
STATE=${1:-""}

# Parallelization parameters
MIN_BATCH_SIZE=${MIN_BATCH_SIZE:-10}
MAX_BATCH_SIZE=${MAX_BATCH_SIZE:-30}
MIN_DELAY=${MIN_DELAY:-2.0}
MAX_DELAY=${MAX_DELAY:-8.0}
MAX_CONCURRENT=${MAX_CONCURRENT:-15}

IFS=$'\n';

# Determine search directory based on state parameter
if [ -n "$STATE" ]; then
  SEARCH_DIR="raw/search_${STATE}"
  CAF_DIR="raw/caf_${STATE}"
  URL_FILE="urls_${STATE}.txt"
  echo "Processing data for state: $STATE"
else
  SEARCH_DIR="raw/search"
  CAF_DIR="raw/caf"
  URL_FILE="urls_all.txt"
  echo "Processing data for all states"
fi

# Check if search directory exists
if [ ! -d "$SEARCH_DIR" ]; then
  echo "Error: Search directory $SEARCH_DIR not found. Please run initialize.sh first."
  exit 1
fi

mkdir -p "$CAF_DIR"

# Check if Python and required modules are available
if ! command -v python3 &> /dev/null; then
  echo "Error: python3 is required for parallel downloading"
  exit 1
fi

# Check if aiohttp is available
if ! python3 -c "import aiohttp" 2>/dev/null; then
  echo "Error: aiohttp module is required. Install with: pip install aiohttp"
  exit 1
fi

echo "Generating URL list for parallel downloading..."

# Remove old URL file if it exists
rm -f "$URL_FILE"

# First pass: count total proposals and generate URL file
total_proposals_all=0
for clearance in {1..4}; do
  SEARCH_FILE="$SEARCH_DIR/${clearance}.json"
  
  if [ ! -s "$SEARCH_FILE" ]; then
    echo "Warning: No data file found at $SEARCH_FILE, skipping clearance type $clearance"
    continue
  fi
  
  mkdir -p "$CAF_DIR/${clearance}"
  
  echo "Processing clearance type $clearance..."
  
  # Extract all proposal IDs and generate URLs
  proposal_ids=$(cat "$SEARCH_FILE" | jq -r '.data[] | select(.id != null) | .id' 2>/dev/null || echo "")
  
  if [ -z "$proposal_ids" ]; then
    echo "  No valid proposal IDs found in $SEARCH_FILE"
    continue
  fi
  
  for proposal_id in $proposal_ids; do
    url="https://parivesh.nic.in/parivesh_api/proponentApplicant/getCafDataByProposalNo?proposal_id=${proposal_id}"
    output_path="$CAF_DIR/${clearance}/${proposal_id}.json"
    
    # Add to URL file (tab-separated)
    echo -e "${url}\t${output_path}" >> "$URL_FILE"
    total_proposals_all=$((total_proposals_all + 1))
  done
done

echo "Generated URL list with $total_proposals_all proposals"
echo ""

# Check if URL file was created and has content
if [ ! -s "$URL_FILE" ]; then
  echo "Error: No URLs generated. Check your search files."
  exit 1
fi

# Run the parallel downloader
echo "Starting parallel download with configuration:"
echo "  Batch size: $MIN_BATCH_SIZE-$MAX_BATCH_SIZE"
echo "  Delay between batches: ${MIN_DELAY}s-${MAX_DELAY}s" 
echo "  Max concurrent downloads: $MAX_CONCURRENT"
echo ""

python3 request.py "$URL_FILE" \
  --min-batch-size "$MIN_BATCH_SIZE" \
  --max-batch-size "$MAX_BATCH_SIZE" \
  --min-delay "$MIN_DELAY" \
  --max-delay "$MAX_DELAY" \
  --max-concurrent "$MAX_CONCURRENT"

download_exit_code=$?

# Clean up URL file
rm -f "$URL_FILE"

if [ $download_exit_code -eq 0 ]; then
  echo ""
  echo "Data fetching completed successfully. Files saved to: $CAF_DIR"
else
  echo ""
  echo "Warning: Parallel download script exited with code $download_exit_code"
  echo "Some downloads may have failed. You may want to re-run this script."
fi
