package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class FluxQueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        FluxQuery query = FluxQuery.builder()
            .connection(InfluxDBConnection.builder()
                .url(Property.ofValue("http://localhost:8086"))
                .token(Property.ofValue("my-token"))
                .build())
            .org(Property.ofValue("my-org"))
            .query(Property.ofValue("from(bucket: \"test-bucket\") |> range(start: -1h) |> limit(n: 10)"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        FluxQuery.Output output = query.run(runContext);

        assertThat(output.getSize(), is(greaterThan(0)));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getTotal(), is(greaterThan(0L)));
    }
}