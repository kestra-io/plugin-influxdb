package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
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
import okhttp3.OkHttpClient;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

    @Schema(
        title = "Connection timeout.",
        description = "Connection establishment timeout (ISO-8601 duration, e.g. `PT10S`). Default is 10 seconds."
    )
    protected Property<Duration> connectTimeout;

    @Schema(
        title = "Read timeout.",
        description = "Maximum time to wait for response bytes (including initial response) (ISO-8601 duration, e.g. `PT10S`). Default is 10 seconds."
    )
    protected Property<Duration> readTimeout;

    protected InfluxDBClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(url).as(String.class).orElseThrow();
        String renderedToken = runContext.render(token).as(String.class).orElseThrow();

        if (connectTimeout == null && readTimeout == null) {
            return InfluxDBClientFactory.create(renderedUrl, renderedToken.toCharArray());
        }

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
        renderTimeout(runContext, connectTimeout).ifPresent(duration -> okHttpClient.connectTimeout(duration.toMillis(), TimeUnit.MILLISECONDS));
        renderTimeout(runContext, readTimeout).ifPresent(duration -> okHttpClient.readTimeout(duration.toMillis(), TimeUnit.MILLISECONDS));

        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
            .url(renderedUrl)
            .authenticateToken(renderedToken.toCharArray())
            .okHttpClient(okHttpClient)
            .build();

        return InfluxDBClientFactory.create(options);
    }

    private static Optional<Duration> renderTimeout(RunContext runContext, Property<Duration> timeout) throws IllegalVariableEvaluationException {
        if (timeout == null) {
            return Optional.empty();
        }

        Duration rendered = runContext.render(timeout).as(Duration.class).orElse(null);
        if (rendered == null) {
            return Optional.empty();
        }

        if (rendered.isNegative() || rendered.isZero()) {
            throw new IllegalArgumentException("Timeout durations must be > 0");
        }

        return Optional.of(rendered);
    }
}
