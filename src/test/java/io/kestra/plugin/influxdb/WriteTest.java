package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

@KestraTest
class WriteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Write task = Write.builder()
            .connection(InfluxDBConnection.builder()
                .url(Property.ofValue("http://localhost:8086"))
                .token(Property.ofValue("my-token"))
                .build())
            .org(Property.ofValue("my-org"))
            .bucket(Property.ofValue("test-bucket"))
            .source(Property.ofValue("""
                airSensors,sensor_id=KLM0100 temperature=71.21211174013729,humidity=35.12317300691224,co=0.48881420596033176
                airSensors,sensor_id=KLM0200 temperature=71.21211174013729,humidity=35.12317300691224,co=0.48881420596033176

            """))
            .build();

        Write.Output output = task.run(runContext);
        assertThat(output.getRecordCount(), is(2));
    }
}
