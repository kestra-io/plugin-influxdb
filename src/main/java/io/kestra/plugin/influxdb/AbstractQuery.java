package io.kestra.plugin.influxdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractQuery extends AbstractTask {
    @Schema(
        title = "Query to execute",
        description = "Query to run against InfluxDB"
    )
    @NotNull
    protected Property<String> query;

    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    protected URI storeResults(RunContext runContext, List<Map<String, Object>> results) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<Map<String, Object>> recordFlux = Flux.fromIterable(results);
            FileSerde.writeAll(output, recordFlux).block();
            return runContext.storage().putFile(tempFile);
        }
    }

    protected Output handleFetchType(RunContext runContext, List<Map<String, Object>> allResults) throws Exception {
        Output.OutputBuilder outputBuilder = Output.builder()
            .total((long) allResults.size());

        FetchType type = runContext.render(fetchType).as(FetchType.class).orElseThrow();

        switch (type) {
            case FETCH:
                outputBuilder
                    .rows(allResults)
                    .size(allResults.size());
                break;

            case FETCH_ONE:
                Map<String, Object> firstRow = allResults.isEmpty() ? null : allResults.getFirst();
                outputBuilder
                    .row(firstRow)
                    .size(firstRow != null ? 1 : 0);
                break;

            case STORE:
                URI uri = storeResults(runContext, allResults);
                outputBuilder
                    .uri(uri)
                    .size(allResults.size());
                break;

            case NONE:
                outputBuilder.size(allResults.size());
                break;
        }

        return outputBuilder.build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The number of rows fetched."
        )
        private Integer size;

        @Schema(
            title = "The total number of the rows fetched without pagination."
        )
        private Long total;

        @Schema(
            title = "List containing the fetched data.",
            description = "Only populated if using `fetchType=FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Map containing the first row of fetched data.",
            description = "Only populated if using `fetchType=FETCH_ONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The URI of the stored data.",
            description = "Only populated if using `fetchType=STORE`."
        )
        private URI uri;
    }
}