package io.kestra.plugin.influxdb;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.DateUtils;
import io.kestra.plugin.influxdb.utils.TimeUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.plugin.influxdb.utils.TimeUtils.toInstant;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data points to InfluxDB from a file.",
    description = "Load data points to InfluxDB from an ION file where each record becomes a data point."
)
@Plugin(
    examples = {
        @Example(
            title = "Load data points to InfluxDB from an ION file.",
            full = true,
            code = """
                id: influxdb_load
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: load
                    type: io.kestra.plugin.influxdb.Load
                    connection:
                      url: "{{ secret('INFLUXDB_URL') }}"
                      token: "{{ secret('INFLUXDB_TOKEN') }}"
                    org: "{{ secret('INFLUXDB_ORG') }}"
                    bucket: "{{ secret('INFLUXDB_BUCKET') }}"
                    from: "{{ inputs.file }}"
                    measurement: "sensor_data"
                """
        )
    }
)
public class Load extends AbstractLoad {
    @Schema(
        title = "Measurement name",
        description = "The measurement name to be used for all points from the ION file"
    )
    @NotNull
    private Property<String> measurement;

    @Schema(
        title = "List of field names to use as tags",
        description = "Fields listed here will be added as tags; all others will be added as fields"
    )
    private Property<List<String>> tags;

    @Schema(
        title = "Field name to use as timestamp",
        description = "The field containing timestamp values. If null, InfluxDB will use the current time."
    )
    private Property<String> timeField;

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<Point> source(RunContext runContext, BufferedReader inputStream) throws Exception {
        String renderedMeasurement = runContext.render(measurement).as(String.class).orElseThrow();
        String renderedTimeField = runContext.render(timeField).as(String.class).orElse(null);
        List<String> renderedTags = runContext.render(tags).asList(String.class);

        return FileSerde.readAll(inputStream)
            .map(throwFunction(data -> {
                Map<String, Object> values = (Map<String, Object>) data;
                Point point = Point.measurement(renderedMeasurement);

                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    boolean isExplicitTimeField = key.equals(renderedTimeField);
                    boolean isImplicitTimeField = renderedTimeField == null && "time".equalsIgnoreCase(key);

                    if (isExplicitTimeField || isImplicitTimeField) {
                        continue;
                    }

                    if (renderedTags != null && renderedTags.contains(key)) {
                        point.addTag(key, value == null ? null : value.toString());
                    } else {
                        switch (value) {
                            case String s -> {
                                try {
                                    double parsed = Double.parseDouble(s);
                                    point.addField(key, parsed);
                                } catch (NumberFormatException e) {
                                    point.addField(key, s);
                                }
                            }
                            case Boolean b -> point.addField(key, b);
                            case null, default -> point.addField(key, Objects.requireNonNull(value).toString());
                        }
                    }

                    if (renderedTimeField != null && values.containsKey(renderedTimeField)) {
                        Object timeValue = values.get(renderedTimeField);
                        point.time(toInstant(timeValue), WritePrecision.NS);
                    }
                }

                return point;
            }));
    }
}