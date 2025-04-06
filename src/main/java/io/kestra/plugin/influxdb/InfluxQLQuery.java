package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxQLQueryApi;
import com.influxdb.query.InfluxQLQueryResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query InfluxDB using InfluxQL",
    description = "Execute an InfluxQL query against InfluxDB and return the results"
)
@Plugin(
    examples = {
        @Example(
            title = "Execute an InfluxQL query and store results in execution",
            full = true,
            code = """
                id: influxdb_query
                namespace: company.team

                tasks:
                  - id: query_influxdb
                    type: io.kestra.plugin.influxdb.InfluxQLQuery
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "my-org"
                    query: "SELECT * FROM measurement WHERE time > now() - 1h"
                    fetchType: EXECUTION
                """
        ),
        @Example(
            title = "Execute an InfluxQL query and store results as ION file",
            full = true,
            code = """
                id: influxdb_query_to_file
                namespace: company.team

                tasks:
                  - id: query_to_file
                    type: io.kestra.plugin.influxdb.InfluxQLQuery
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "my-org"
                    query: "SELECT * FROM measurement WHERE time > now() - 1h"
                    fetchType: ION
                """
        )
    }
)
public class InfluxQLQuery extends AbstractTask implements RunnableTask<InfluxQLQuery.Output> {
    @Schema(
        title = "InfluxQL query to execute",
        description = "The InfluxQL query to execute against InfluxDB"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Property<String> query;

    @Schema(
        title = "Result fetch type",
        description = "Determines how the query results are stored",
        defaultValue = "EXECUTION"
    )
    @PluginProperty
    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.EXECUTION);

    public enum FetchType {
        EXECUTION,
        ION
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (InfluxDBClient client = client(runContext)) {
            String renderedQuery = runContext.render(query).as(String.class).orElseThrow();
            String renderedBucket = runContext.render(bucket).as(String.class).orElseThrow();

            InfluxQLQueryApi queryApi = client.getInfluxQLQueryApi();

            InfluxQLQueryResult result = queryApi.query(
                new com.influxdb.client.domain.InfluxQLQuery(renderedQuery, renderedBucket)
            );

            int recordCount = 0;
            List<Map<String, Object>> results = new ArrayList<>();

            for (InfluxQLQueryResult.Result table : result.getResults()) {
                for (InfluxQLQueryResult.Series series : table.getSeries()) {
                    recordCount++;
                    Map<String, Integer> columnIndexMap = series.getColumns();

                    Map<Integer, String> indexToColumnName = columnIndexMap.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

                    for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                        Object[] values = record.getValues();
                        Map<String, Object> rowMap = new HashMap<>();

                        for (int i = 0; i < values.length; i++) {
                            String colName = indexToColumnName.get(i);
                            if (colName != null && values[i] != null) {
                                rowMap.put(colName, values[i]);
                            }
                        }

                        results.add(rowMap);
                    }
                }
            }


            runContext.metric(Counter.of("records", recordCount));

            URI uri = null;
            if (FetchType.ION.equals(runContext.render(fetchType).as(FetchType.class).orElseThrow())) {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                uri = runContext.storage().putFile(tempFile);
            }

            return Output.builder()
                .uri(uri)
                .rows(FetchType.EXECUTION.equals(runContext.render(fetchType).as(FetchType.class).orElseThrow()) ? results : null)
                .size(recordCount)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the ION file containing query results",
            description = "Only available when fetchType is ION"
        )
        private final URI uri;

        @Schema(
            title = "Query results as a list of maps",
            description = "Only available when fetchType is EXECUTION"
        )
            private final List<Map<String, Object>> rows;

        @Schema(
            title = "Number of records returned by the query"
        )
        private final Integer size;
    }
}