package io.kestra.plugin.influxdb;

import com.influxdb.client.write.Point;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data points to InfluxDB from a file",
    description = "Load data points to InfluxDB from an ION file where each record becomes a data point"
)
@Plugin(
    examples = {
        @Example(
            title = "Load data points to InfluxDB from an ION file",
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

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<Point> source(RunContext runContext, BufferedReader inputStream) throws Exception {
        String renderedMeasurement = runContext.render(measurement).as(String.class).orElseThrow();

        return FileSerde.readAll(inputStream)
            .map(throwFunction(data -> {
                Map<String, Object> values = (Map<String, Object>) data;
                Map<String, Object> fields = (Map<String, Object>) values.get("fields");
                Map<String, String> tags = (Map<String, String>) values.get("tags");

                return Point.measurement(renderedMeasurement).addFields(fields).addTags(tags);
            }));

    }
}