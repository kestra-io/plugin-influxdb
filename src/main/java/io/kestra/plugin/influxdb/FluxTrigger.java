package io.kestra.plugin.influxdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on a Flux query of InfluxDB that returns results."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for an InfluxDB Flux query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: influxdb_trigger
                namespace: company.team
                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ json(taskrun.value) }}"
                triggers:
                  - id: watch
                    type: io.kestra.plugin.influxdb.FluxTrigger
                    interval: "PT5M"
                    connection:
                        url: "{{ secret('INFLUXDB_URL') }}"
                        token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "{{ secret('INFLUXDB_ORG') }}"
                    query: |
                      from(bucket: "mybucket")
                        |> range(start: -1h)
                        |> filter(fn: (r) => r._measurement == "cpu")
                        |> filter(fn: (r) => r._field == "usage_system")
                        |> filter(fn: (r) => r._value > 80.0)
                """
        )
    }
)
public class FluxTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<FluxQuery.Output> {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private InfluxDBConnection connection;

    private Property<String> org;

    private Property<String> bucket;

    private Property<String> query;

    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.NONE);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        FluxQuery fluxQuery = FluxQuery.builder()
            .id(this.id)
            .type(FluxQuery.class.getName())
            .connection(this.connection)
            .org(this.org)
            .query(this.query)
            .fetchType(this.fetchType)
            .build();

        FluxQuery.Output output = fluxQuery.run(runContext);
        logger.debug("Found '{}' rows", output.getSize());

        if (Optional.ofNullable(output.getSize()).orElse(0) == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }
}