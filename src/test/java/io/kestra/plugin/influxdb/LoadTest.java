package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class LoadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (int i = 0; i < 5; i++) {
                FileSerde.write(output, ImmutableMap.of(
                    "sensor", "sensor-" + i,
                    "value", i * 10,
                    "location", "room-" + (i % 2),
                    "time", Instant.now().toString()
                ));
            }
        }

        URI fileUri = runContext.storage().putFile(tempFile);

        Load task = Load.builder()
            .connection(InfluxDBConnection.builder()
                .url(Property.of("http://localhost:8086"))
                .token(Property.of("my-token"))
                .build())
            .org(Property.of("my-org"))
            .bucket(Property.of("test-bucket"))
            .from(Property.of(fileUri.toString()))
            .measurement(Property.of("sensor_data"))
            .tags(Property.of(List.of("sensor", "location")))
            .timeField(Property.of("time"))
            .build();

        Load.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getRecordCount(), is(5));
        assertThat(runContext.metrics().stream().anyMatch(m -> m.getName().equals("records")), is(true));
    }
}
