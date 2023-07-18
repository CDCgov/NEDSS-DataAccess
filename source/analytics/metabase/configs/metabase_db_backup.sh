#!/bin/bash

# Get the current time
timestamp=$(date +%Y%m%d_%H%M%S)

mkdir -p /home/ubuntu/h2_metabase_bkup/$timestamp

sudo docker cp metabase:metabase-data/metabase.db /home/ubuntu/h2_metabase_bkup/$timestamp/.

echo "Backup created for :" $timestamp