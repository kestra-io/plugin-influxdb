package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxQLQueryApi;
import com.influxdb.query.InfluxQLQueryResult;
import io.kestra.core.models.annotations.Example;
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
import java.util.stream.Collectors;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query InfluxDB using InfluxQL",
    description = "Execute an InfluxQL query against InfluxDB"
)
@Plugin(
    examples = {
        @Example(
            title = "Execute an InfluxQL query",
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

            InfluxQLQueryResult result = queryApi.query(
                new com.influxdb.client.domain.InfluxQLQuery(renderedQuery, renderedBucket)
            );

            List<Map<String, Object>> results = new ArrayList<>();

            for (InfluxQLQueryResult.Result table : result.getResults()) {
                for (InfluxQLQueryResult.Series series : table.getSeries()) {
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

            runContext.metric(Counter.of("records", results.size()));

            Output.OutputBuilder outputBuilder = handleFetchType(runContext, results);

            return outputBuilder
                .total((long) results.size())
                .build();
        }
    }
}