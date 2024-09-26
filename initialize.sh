#!/bin/bash

mkdir -p "raw/search"

for clearance in {1..4};
do
  curl "https://parivesh.nic.in/parivesh_api/trackYourProposal/advanceSearchData?majorClearanceType=${clearance}&state=&sector=&proposalStatus=&proposalType=&issuingAuthority=&activityId=&category=&startDate=&endDate=&areaMin=&areaMax=&text=" > "raw/search/${clearance}.json"
  sleep 60
done
