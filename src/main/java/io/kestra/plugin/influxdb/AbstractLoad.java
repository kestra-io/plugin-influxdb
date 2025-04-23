package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for loading data to InfluxDB from files
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLoad extends AbstractTask implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "The source file URI",
        description = "URI of the file containing data to be loaded into InfluxDB"
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Chunk size for each bulk write request",
        description = "Number of points to include in each write batch"
    )
    @Builder.Default
    private Property<Integer> chunk = Property.of(1000);

    /**
     * Abstract method to transform input data into Points
     *
     * @param runContext the run context
     * @param inputStream the source data
     * @return a Flux of Points
     */
    protected abstract Flux<Point> source(RunContext runContext, BufferedReader inputStream) throws Exception;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = URI.create(runContext.render(this.from).as(String.class).orElseThrow());
        AtomicInteger count = new AtomicInteger();

        try (
            InfluxDBClient client = this.connection.client(runContext);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)
        ) {
            WriteApiBlocking writeApi = client.getWriteApiBlocking();
            String renderedBucket = runContext.render(bucket).as(String.class).orElseThrow();
            String renderedOrg = runContext.render(org).as(String.class).orElseThrow();
            Integer renderedChunk = runContext.render(this.chunk).as(Integer.class).orElse(1000);

            Mono<Long> result = this.source(runContext, inputStream)
                .doOnNext(point -> count.incrementAndGet())
                .buffer(renderedChunk)
                .map(points -> {
                    List<Point> batch = new ArrayList<>(points);
                    writeApi.writePoints(renderedBucket, renderedOrg, batch);

                    logger.debug("Wrote batch of {} points", batch.size());
                    return batch.size();
                })
                .count();

            Long batchCount = result.block();
            runContext.metric(Counter.of("batches.count", batchCount));
            runContext.metric(Counter.of("records", count.get()));

            logger.info(
                "Successfully sent {} batches for {} records",
                batchCount,
                count.get()
            );

            return Output.builder()
                .recordCount(count.get())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of records written to InfluxDB")
        private final Integer recordCount;
    }
}