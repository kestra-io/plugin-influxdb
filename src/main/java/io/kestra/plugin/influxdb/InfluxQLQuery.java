package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxQLQueryApi;
import com.influxdb.query.InfluxQLQueryResult;
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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run InfluxQL query against InfluxDB",
    description = "Executes an InfluxQL query for the specified bucket and organization. `fetchType` controls whether rows are returned inline, stored, or just counted."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute an InfluxQL query.",
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
                    fetchType: FETCH
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
public class InfluxQLQuery extends AbstractQuery implements RunnableTask<AbstractQuery.Output> {
    @Override
    public AbstractQuery.Output run(RunContext runContext) throws Exception {
        try (InfluxDBClient client = client(runContext)) {
            Logger logger = runContext.logger();
            String renderedQuery = runContext.render(query).as(String.class).orElseThrow();
            String renderedBucket = runContext.render(bucket).as(String.class).orElseThrow();

            InfluxQLQueryApi queryApi = client.getInfluxQLQueryApi();

            logger.debug("Starting query: {}", query);

            InfluxQLQueryResult queryResult = queryApi.query(
                new com.influxdb.client.domain.InfluxQLQuery(renderedQuery, renderedBucket)
            );

            List<Map<String, Object>> results = Optional.of(queryResult.getResults())
                .orElse(Collections.emptyList())
                .stream()
                .filter(Objects::nonNull)
                .flatMap(table -> Optional.of(table.getSeries())
                    .orElse(Collections.emptyList())
                    .stream()
                    .filter(Objects::nonNull))
                .flatMap(series -> {
                    Map<String, Integer> columns = series.getColumns();
                    if (columns.isEmpty()) {
                        return Stream.empty();
                    }

                    Map<Integer, String> indexToColumn = new HashMap<>();
                    columns.forEach((name, index) -> indexToColumn.put(index, name));

                    return Optional.of(series.getValues())
                        .orElse(Collections.emptyList())
                        .stream()
                        .filter(Objects::nonNull)
                        .map(record -> {
                            Object[] values = record.getValues();
                            if (values == null) {
                                return Collections.<String, Object>emptyMap();
                            }

                            Map<String, Object> row = new HashMap<>();
                            for (int i = 0; i < values.length; i++) {
                                if (values[i] != null) {
                                    String colName = indexToColumn.get(i);
                                    if (colName != null) {
                                        row.put(colName, values[i]);
                                    }
                                }
                            }
                            return row;
                        })
                        .filter(map -> !map.isEmpty());
                })
                .collect(Collectors.toList());

            runContext.metric(Counter.of("records", results.size()));

            return handleFetchType(runContext, results);
        }
    }
}
