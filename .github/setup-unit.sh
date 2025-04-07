#!/bin/bash

cd "$(dirname "$0")/.."
docker compose -f docker-compose-ci.yml up -d
sleep 10
curl -s -o .github/workflows/influxdb/air-sensor-data.lp https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/air-sensor-data/air-sensor-data.lp
docker compose -f docker-compose-ci.yml exec influxdb sh -c ' influx write --bucket test-bucket --org my-org --token my-token --precision s --file /docker-entrypoint-initdb.d/air-sensor-data.lp'

