#!/bin/bash

# Navigate to the root directory
cd "$(dirname "$0")/.."

# Start the Docker containers in the background
docker compose -f docker-compose-ci.yml up -d
sleep 10  # Wait for containers to initialize

# Fetch the air sensor data
curl -s -o .github/workflows/air-sensor-data.lp https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/air-sensor-data/air-sensor-data.lp

# Write the fetched data to InfluxDB
docker compose -f docker-compose-ci.yml exec influxdb sh -c 'influx write --bucket test-bucket --precision ns --file /docker-entrypoint-initdb.d/air-sensor-data.lp'

# Query data to verify it was written
result=$(docker compose -f docker-compose-ci.yml exec influxdb sh -c 'influx query "from(bucket: \"test-bucket\") |> range(start: -1h) |> count()"')

# Check if the query result is greater than 0, indicating data was written
if [[ $result =~ "count" && ${result#*:} -gt 0 ]]; then
  echo "Data was successfully written to InfluxDB!"
else
  echo "No data found in InfluxDB. There may have been an issue with writing the data."
fi
