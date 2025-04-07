package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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
                .url(Property.of("http://localhost:8086"))
                .token(Property.of("my-token"))
                .build())
            .org(Property.of("my-org"))
            .query(Property.of("from(bucket: \"test-bucket\") |> range(start: -1h) |> limit(n: 10)"))
            .fetchType(Property.of(FluxQuery.FetchType.FETCH))
            .build();

        FluxQuery.Output output = query.run(runContext);

        assertThat(output.getSize(), is(greaterThan(0)));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getTotal(), is(greaterThan(0L)));
    }
}