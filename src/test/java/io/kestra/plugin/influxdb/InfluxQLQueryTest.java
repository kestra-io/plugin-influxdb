package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class InfluxQLQueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        InfluxQLQuery query = InfluxQLQuery.builder()
            .connection(InfluxDBConnection.builder()
                .url(Property.of("http://localhost:8086"))
                .token(Property.of("my-token"))
                .build())
            .org(Property.of("my-org"))
            .bucket(Property.of("test-bucket"))
            .query(Property.of("SELECT * FROM airSensors LIMIT 5"))
            .fetchType(Property.of(InfluxQLQuery.FetchType.EXECUTION))
            .build();

        InfluxQLQuery.Output output = query.run(runContext);

        assertThat(output.getSize(), is(greaterThanOrEqualTo(0)));
        assertThat(output.getRows(), is(notNullValue()));
        if (!output.getRows().isEmpty()) {
            assertThat(output.getRows().getFirst(), is(instanceOf(Map.class)));
        }
    }
}
