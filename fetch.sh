#!/bin/bash

IFS=$'\n';

mkdir -p "raw/caf"

for clearance in {1..4};
do
  mkdir -p "raw/caf/${clearance}"

  for proposal in $(cat "raw/search/${clearance}.json" | jq -c '.data[]');
  do

    proposal_id=$(echo "$proposal" | jq -r '.id')

    if ! [ -f "raw/caf/${clearance}/${proposal_id}.json" ]; then
      curl -X POST "https://parivesh.nic.in/parivesh_api/proponentApplicant/getCafDataByProposalNo?proposal_id=${proposal_id}" > "raw/caf/${clearance}/${proposal_id}.json"
      sleep 15
    fi

    proposal_updated_on=$(echo "$proposal" | jq -r '.app_updated_on')

    if [[ "$proposal_updated_on" != "null" ]]; then

      proposal_updated_on=$(date -d "$proposal_updated_on" +%s)
      file_date=$(date +%s -r "raw/caf/${clearance}/${proposal_id}.json")

      if [ $file_date -lt $proposal_updated_on ]; then
        echo "Refetching $proposal_id due to application update since last fetch (updated: $proposal_updated_on, fetch: $file_date)"
        curl -X POST "https://parivesh.nic.in/parivesh_api/proponentApplicant/getCafDataByProposalNo?proposal_id=${proposal_id}" > "raw/caf/${clearance}/${proposal_id}.json"
        sleep 15
      fi
    fi
  done
done
