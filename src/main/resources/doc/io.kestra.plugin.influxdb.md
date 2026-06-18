# How to use the InfluxDB plugin

Query and write time-series data in InfluxDB from Kestra flows.

## Authentication

Configure the `connection` object: set `url` to your InfluxDB server URL and `token` to your authentication token. Set `org` and `bucket` at the task level to scope operations. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`FluxQuery` runs a Flux query set in `query` against the specified `org`. The bucket is embedded in the Flux query string — the task-level `bucket` property is ignored. Control result handling with `fetchType`: `NONE` (default), `FETCH`, `FETCH_ONE`, or `STORE`.

`InfluxQLQuery` runs an InfluxQL query set in `query`. The `bucket` property is required for InfluxQL queries. Supports the same `fetchType` options as `FluxQuery`.

`Write` writes data in line protocol format — set `source` to the line protocol payload. Control timestamp precision with `precision` (default `NS` for nanoseconds).

`Load` bulk-loads data from a file in internal storage — set `from` to a `kestra://` URI and `measurement` to the measurement name applied to all points. Use `tags` to designate which fields become tags and `timeField` to map a field to the point timestamp. Control batch size with `chunk` (default 1000).

`FluxTrigger` polls InfluxDB on a schedule (default 60 seconds) using a Flux `query` and starts one execution per batch of results.
