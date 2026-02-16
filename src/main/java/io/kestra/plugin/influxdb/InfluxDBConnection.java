package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
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
    @Builder.Default
    protected Property<Duration> connectTimeout = Property.ofValue(Duration.ofSeconds(10));

    @Schema(
        title = "Read timeout.",
        description = "Maximum time to wait for response bytes (including initial response) (ISO-8601 duration, e.g. `PT10S`). Default is 10 seconds."
    )
    @Builder.Default
    protected Property<Duration> readTimeout = Property.ofValue(Duration.ofSeconds(10));

    protected InfluxDBClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(url).as(String.class).orElseThrow();
        String renderedToken = runContext.render(token).as(String.class).orElseThrow();
        Duration rConnectTimeout = runContext.render(connectTimeout).as(Duration.class).orElse(Duration.ofSeconds(10));
        Duration rReadTimeout = runContext.render(readTimeout).as(Duration.class).orElse(Duration.ofSeconds(10));

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
        okHttpClient.connectTimeout(rConnectTimeout.toMillis(), TimeUnit.MILLISECONDS);
        okHttpClient.readTimeout(rReadTimeout.toMillis(), TimeUnit.MILLISECONDS);

        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
            .url(renderedUrl)
            .authenticateToken(renderedToken.toCharArray())
            .okHttpClient(okHttpClient)
            .build();

        return InfluxDBClientFactory.create(options);
    }
}
