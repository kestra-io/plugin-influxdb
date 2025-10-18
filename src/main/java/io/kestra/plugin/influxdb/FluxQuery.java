package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query InfluxDB using Flux language.",
    description = "Execute a Flux query against InfluxDB."
)
@Plugin(
    examples = {
        @Example(
            title = "Query data from InfluxDB and store as ION file",
            full = true,
            code = """
                id: influxdb_flux_query
                namespace: company.team

                tasks:
                  - id: query_influxdb
                    type: io.kestra.plugin.influxdb.FluxQuery
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "my-org"
                    query: |
                      from(bucket: "my-bucket")
                        |> range(start: -1h)
                        |> filter(fn: (r) => r._measurement == "cpu")
                        |> yield()
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Query data from InfluxDB and return all rows in execution output.",
            full = true,
            code = """
                id: influxdb_flux_query_inline
                namespace: company.team

                tasks:
                  - id: query_influxdb
                    type: io.kestra.plugin.influxdb.FluxQuery
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "my-org"
                    query: |
                      from(bucket: "my-bucket")
                        |> range(start: -1h)
                        |> filter(fn: (r) => r._measurement == "cpu")
                        |> limit(n: 10)
                        |> yield()
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Query data from InfluxDB and return only the first row.",
            full = true,
            code = """
                id: influxdb_flux_query_one
                namespace: company.team

                tasks:
                  - id: query_influxdb
                    type: io.kestra.plugin.influxdb.FluxQuery
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "my-org"
                    query: |
                      from(bucket: "my-bucket")
                        |> range(start: -1h)
                        |> filter(fn: (r) => r._measurement == "cpu")
                        |> limit(n: 1)
                        |> yield()
                    fetchType: FETCH_ONE
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "count",
            description = "The number of records returned by the query"
        )
    }
)
public class FluxQuery extends AbstractQuery implements RunnableTask<AbstractQuery.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedQuery = runContext.render(query).as(String.class).orElseThrow();
        String renderedOrg = runContext.render(org).as(String.class).orElseThrow();

        try (InfluxDBClient client = this.connection.client(runContext)) {
            QueryApi queryApi = client.getQueryApi();
            logger.debug("Starting query: {}", query);

            if (bucket != null) {
                logger.info("Bucket is ignored for FluxQuery as it's embedded in the query string.");
            }

            List<FluxTable> tables = queryApi.query(renderedQuery, renderedOrg);
            List<Map<String, Object>> results = new ArrayList<>();

            for (FluxTable table : tables) {
                List<FluxRecord> records = table.getRecords();

                for (FluxRecord record : records) {
                    Map<String, Object> row = new HashMap<>();
                    record.getValues().forEach((key, value) -> {
                        if (value != null) {
                            row.put(key, value);
                        }
                    });
                    results.add(row);
                }
            }

            runContext.metric(Counter.of("records", results.size()));

            return handleFetchType(runContext, results);
        }
    }
}