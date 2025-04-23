package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
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
public class InfluxDBConnection {
    @Schema(
        title = "InfluxDB server URL",
        description = "The URL of the InfluxDB server"
    )
    @NotNull
    protected Property<@NotEmpty String> url;

    @Schema(
        title = "InfluxDB token",
        description = "The authentication token for InfluxDB"
    )
    @NotNull
    protected Property<@NotEmpty String> token;

    protected InfluxDBClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(url).as(String.class).orElseThrow();
        String renderedToken = runContext.render(token).as(String.class).orElseThrow();
        return InfluxDBClientFactory.create(renderedUrl, renderedToken.toCharArray());
    }
}