package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
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
                .url(Property.ofValue("http://localhost:8086"))
                .token(Property.ofValue("my-token"))
                .build())
            .org(Property.ofValue("my-org"))
            .bucket(Property.ofValue("test-bucket"))
            .query(Property.ofValue("SELECT * FROM airSensors LIMIT 5"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        InfluxQLQuery.Output output = query.run(runContext);

        assertThat(output.getSize(), is(greaterThanOrEqualTo(0)));
        assertThat(output.getRows(), is(notNullValue()));
        if (!output.getRows().isEmpty()) {
            assertThat(output.getRows().getFirst(), is(instanceOf(Map.class)));
        }
    }
}
