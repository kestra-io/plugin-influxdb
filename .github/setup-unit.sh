#!/bin/bash

cd "$(dirname "$0")/.."
docker compose -f docker-compose-ci.yml up -d
sleep 10
timestamp=$(($(date +%s)*1000000000))
cat > .github/workflows/air-sensor-temp.lp << EOF
air,sensor_id=TLM0100 temperature=73.97 $timestamp
air,sensor_id=TLM0100 humidity=35.23 $timestamp
air,sensor_id=TLM0100 co=0.48 $timestamp
EOF
docker compose -f docker-compose-ci.yml exec influxdb sh -c 'influx write --bucket test-bucket --org my-org --token my-token --precision ns --file /docker-entrypoint-initdb.d/air-sensor-temp.lp'
