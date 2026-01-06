package io.kestra.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class FluxReadTimeoutTest {
    private static final String FLUX_CSV_RESPONSE = """
        #datatype,string,long,dateTime:RFC3339,double
        #group,false,false,true,false
        #default,_result,,,
        ,result,table,_time,_value
        ,_result,0,2020-01-01T00:00:00Z,1
        """;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldTimeoutWhenFirstResponseBytesAreDelayedBeyondReadTimeout() throws Exception {
        try (DelayedInfluxServer server = DelayedInfluxServer.start(Duration.ofMillis(750), FLUX_CSV_RESPONSE)) {
            RunContext runContext = runContextFactory.of(ImmutableMap.of());

            FluxQuery query = FluxQuery.builder()
                .connection(InfluxDBConnection.builder()
                    .url(Property.ofValue(server.baseUrl()))
                    .token(Property.ofValue("my-token"))
                    .readTimeout(Property.ofValue(Duration.ofMillis(200)))
                    .build())
                .org(Property.ofValue("my-org"))
                .query(Property.ofValue("from(bucket: \"test-bucket\") |> range(start: -1h) |> limit(n: 1)"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build();

            Exception ex = assertThrows(Exception.class, () -> query.run(runContext));
            assertThat(hasTimeoutCause(ex), is(true));
        }
    }

    @Test
    void shouldSucceedWhenReadTimeoutAllowsDelayedFirstResponseBytes() throws Exception {
        try (DelayedInfluxServer server = DelayedInfluxServer.start(Duration.ofMillis(250), FLUX_CSV_RESPONSE)) {
            RunContext runContext = runContextFactory.of(ImmutableMap.of());

            FluxQuery query = FluxQuery.builder()
                .connection(InfluxDBConnection.builder()
                    .url(Property.ofValue(server.baseUrl()))
                    .token(Property.ofValue("my-token"))
                    .readTimeout(Property.ofValue(Duration.ofSeconds(2)))
                    .build())
                .org(Property.ofValue("my-org"))
                .query(Property.ofValue("from(bucket: \"test-bucket\") |> range(start: -1h) |> limit(n: 1)"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build();

            FluxQuery.Output output = query.run(runContext);

            assertThat(output.getSize(), is(1));
            assertThat(output.getRows(), hasSize(1));
            assertThat(output.getRows().getFirst().get("_value"), is(1.0));
        }
    }

    private static boolean hasTimeoutCause(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof java.net.SocketTimeoutException) {
                return true;
            }
            String message = current.getMessage();
            if (message != null && message.toLowerCase().contains("timeout")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class DelayedInfluxServer implements AutoCloseable {
        private final HttpServer httpServer;
        private final String baseUrl;

        private DelayedInfluxServer(HttpServer httpServer) {
            this.httpServer = httpServer;
            int port = httpServer.getAddress().getPort();
            this.baseUrl = "http://localhost:" + port;
        }

        static DelayedInfluxServer start(Duration bodyDelay, String responseBody) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.setExecutor(Executors.newCachedThreadPool());
            server.createContext("/", exchange -> {
                try {
                    exchange.getRequestBody().readAllBytes();

                    exchange.getResponseHeaders().add("Content-Type", "application/csv; charset=utf-8");
                    exchange.sendResponseHeaders(200, 0);

                    try (OutputStream os = exchange.getResponseBody()) {
                        try {
                            Thread.sleep(bodyDelay.toMillis());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        os.write(responseBody.getBytes(StandardCharsets.UTF_8));
                    }
                } finally {
                    exchange.close();
                }
            });
            server.start();
            return new DelayedInfluxServer(server);
        }

        String baseUrl() {
            return baseUrl;
        }

        @Override
        public void close() {
            httpServer.stop(0);
        }
    }
}
