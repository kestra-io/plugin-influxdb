package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Arrays;

/**
 * Task for writing line protocol data directly to InfluxDB
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write line protocol to InfluxDB",
    description = "Sends raw InfluxDB line protocol to a bucket/org with configurable timestamp precision (default nanoseconds). Counts and reports written lines."
)
@Plugin(
    examples = {
        @Example(
            title = "Write data to InfluxDB using line protocol.",
            full = true,
            code = """
                id: influxdb_write
                namespace: company.team

                tasks:
                  - id: write
                    type: io.kestra.plugin.influxdb.Write
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "my_org"
                    bucket: "my-bucket"
                    source: |
                      measurement,tag=value field=1.0
                      measurement,tag=value2 field=2.0
                """
        )
    },
    metrics = { 
        @Metric(
            name = "records", 
            type = Counter.TYPE,
            unit = "count",
            description = "The number of records written to InfluxDB"
            )
    }
)
public class Write extends AbstractTask implements RunnableTask<Write.Output> {
    @Schema(
        title = "Line protocol payload",
        description = "Multiline string in InfluxDB line protocol"
    )
    @NotNull
    private Property<String> source;

    @Schema(
        title = "Timestamp precision",
        description = "Precision applied to unix timestamps in the payload; defaults to nanoseconds"
    )
    @Builder.Default
    private Property<WritePrecision> precision = Property.ofValue(WritePrecision.NS);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (
            InfluxDBClient client = this.connection.client(runContext);
        ) {
            WriteApiBlocking writeApi = client.getWriteApiBlocking();
            String renderedSource = runContext.render(source).as(String.class).orElseThrow();
            String renderedBucket = runContext.render(bucket).as(String.class).orElseThrow();
            String renderedOrg = runContext.render(org).as(String.class).orElseThrow();
            WritePrecision renderedPrecision = runContext.render(precision).as(WritePrecision.class).orElse(WritePrecision.NS);

            writeApi.writeRecord(renderedBucket, renderedOrg, renderedPrecision, renderedSource);

            int lineCount = (int) Arrays.stream(renderedSource.split("\n"))
                .filter(line -> !line.trim().isEmpty())
                .count();

            logger.info("Wrote {} lines of line protocol data to InfluxDB", lineCount);
            runContext.metric(Counter.of("records", lineCount));

            return Output.builder()
                .recordCount(lineCount)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of records written to InfluxDB")
        private final Integer recordCount;
    }
}
