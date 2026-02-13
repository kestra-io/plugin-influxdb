package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    @Schema(
        title = "InfluxDB connection",
        description = "Connection settings (URL, token, optional options) reused across tasks"
    )
    @NotNull
    protected InfluxDBConnection connection;

    @Schema(
        title = "Bucket name",
        description = "Target bucket for write/query operations when required; ignored by Flux queries"
    )
    protected Property<String> bucket;

    @Schema(
        title = "Organization name",
        description = "Organization scope for queries and writes"
    )
    @NotNull
    protected Property<String> org;

    /**
     * Get a configured InfluxDB client
     *
     * @param runContext The current run context
     * @return A configured InfluxDB client
     * @throws IllegalVariableEvaluationException If variable rendering fails
     */
    protected InfluxDBClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return connection.client(runContext);
    }
}
