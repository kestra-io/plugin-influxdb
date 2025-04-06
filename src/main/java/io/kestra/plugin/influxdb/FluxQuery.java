package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
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
    title = "Query InfluxDB using Flux language",
    description = "Execute a Flux query against InfluxDB and return the results"
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
                    url: "{{ secret('INFLUXDB_URL') }}"
                    token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "{{ secret('INFLUXDB_ORG') }}"
                    query: |
                      from(bucket: "my-bucket")
                        |> range(start: -1h)
                        |> filter(fn: (r) => r._measurement == "cpu")
                        |> yield()
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Query data from InfluxDB and return all rows in execution output",
            full = true,
            code = """
                id: influxdb_flux_query_inline
                namespace: company.team

                tasks:
                  - id: query_influxdb
                    type: io.kestra.plugin.influxdb.FluxQuery
                    url: "{{ secret('INFLUXDB_URL') }}"
                    token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "{{ secret('INFLUXDB_ORG') }}"
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
            title = "Query data from InfluxDB and return only the first row",
            full = true,
            code = """
                id: influxdb_flux_query_one
                namespace: company.team

                tasks:
                  - id: query_influxdb
                    type: io.kestra.plugin.influxdb.FluxQuery
                    url: "{{ secret('INFLUXDB_URL') }}"
                    token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "{{ secret('INFLUXDB_ORG') }}"
                    query: |
                      from(bucket: "my-bucket")
                        |> range(start: -1h)
                        |> filter(fn: (r) => r._measurement == "cpu")
                        |> limit(n: 1)
                        |> yield()
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class FluxQuery extends AbstractTask implements RunnableTask<FluxQuery.Output> {
    @Schema(
        title = "Flux query to execute",
        description = "The Flux query to run against InfluxDB"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Property<String> query;

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file."
    )
    @Builder.Default
    private Property<Boolean> store = Property.of(false);

    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @PluginProperty
    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.FETCH);

    public enum FetchType {
        FETCH,
        FETCH_ONE,
        STORE,
        NONE
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedQuery = runContext.render(query).as(String.class).orElseThrow();

        try (
            InfluxDBClient client = this.connection.client(runContext);
        ){
            QueryApi queryApi = client.getQueryApi();
            logger.debug("Starting query: {}", query);

            List<FluxTable> tables = queryApi.query(renderedQuery);

            Output.OutputBuilder outputBuilder = Output.builder();
            int recordCount = 0;

            for (FluxTable table : tables) {
                recordCount += table.getRecords().size();
            }

            switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
                case FETCH:
                    Pair<List<Map<String, Object>>, Integer> fetch = this.fetch(tables);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                    break;

                case FETCH_ONE:
                    Map<String, Object> row = this.fetchOne(tables);
                    outputBuilder
                        .row(row)
                        .size(row != null ? 1 : 0);
                    break;

                case STORE:
                    Pair<URI, Long> store = this.store(runContext, tables);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight().intValue());
                    break;

                case NONE:
                    outputBuilder.size(recordCount);
                    break;
            }

            runContext.metric(Counter.of("records", recordCount));
            return outputBuilder.total((long) tables.size()).build();
        }
    }

    protected Pair<List<Map<String, Object>>, Integer> fetch(List<FluxTable> tables) {
        List<Map<String, Object>> results = new ArrayList<>();
        int count = 0;

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                count++;

                // Convert FluxRecord to Map with proper Java types
                Map<String, Object> row = new HashMap<>();
                record.getValues().forEach((key, value) -> {
                    if (value != null) {
                        row.put(key, value);
                    }
                });

                results.add(row);
            }
        }

        return Pair.of(results, count);
    }

    protected Map<String, Object> fetchOne(List<FluxTable> tables) {
        if (tables.isEmpty() || tables.get(0).getRecords().isEmpty()) {
            return null;
        }

        FluxRecord record = tables.get(0).getRecords().get(0);
        Map<String, Object> row = new HashMap<>();

        record.getValues().forEach((key, value) -> {
            if (value != null) {
                row.put(key, value);
            }
        });

        return row;
    }

    protected Pair<URI, Long> store(RunContext runContext, List<FluxTable> tables) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<Map<String, Object>> recordFlux = Flux.fromIterable(tables)
                .flatMap(table -> Flux.fromIterable(table.getRecords()))
                .map(record -> {
                    Map<String, Object> row = new HashMap<>();
                    record.getValues().forEach((key, value) -> {
                        if (value != null) {
                            row.put(key, value);
                        }
                    });
                    return row;
                });

            Long count = FileSerde.writeAll(output, recordFlux).block();

                return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the rows fetched."
        )
        private Integer size;

        @Schema(
            title = "The total of the rows fetched without pagination."
        )
        private Long total;

        @Schema(
            title = "List containing the fetched data.",
            description = "Only populated if using `fetchType=FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Map containing the first row of fetched data.",
            description = "Only populated if using `fetchType=FETCH_ONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The URI of the stored data.",
            description = "Only populated if using `fetchType=STORE`."
        )
        private URI uri;
    }
}