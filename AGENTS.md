# Kestra InfluxDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.influxdb`.
- Includes classes such as `FluxQuery`, `InfluxQLQuery`, `Write`, `Load`, and `FluxTrigger`.
- Focuses on querying, writing, loading, and polling InfluxDB data from Kestra workflows.

## Why

- This plugin integrates Kestra with InfluxDB.
- It lets Kestra workflows query time-series data, write points, and react to matching records.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `influxdb`

Infrastructure dependencies (Docker Compose services):

- `influxdb`

### Key Plugin Classes

- `io.kestra.plugin.influxdb.AbstractLoad`
- `io.kestra.plugin.influxdb.FluxQuery`
- `io.kestra.plugin.influxdb.FluxTrigger`
- `io.kestra.plugin.influxdb.InfluxQLQuery`
- `io.kestra.plugin.influxdb.Load`
- `io.kestra.plugin.influxdb.Write`

### Project Structure

```
plugin-influxdb/
├── src/main/java/io/kestra/plugin/influxdb/utils/
├── src/test/java/io/kestra/plugin/influxdb/utils/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
