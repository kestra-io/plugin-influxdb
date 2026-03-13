# Kestra InfluxDB Plugin

## What

description = 'Plugin InfluxDB for Kestra Exposes 6 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with InfluxDB, allowing orchestration of InfluxDB-based operations as part of data pipelines and automation workflows.

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

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
