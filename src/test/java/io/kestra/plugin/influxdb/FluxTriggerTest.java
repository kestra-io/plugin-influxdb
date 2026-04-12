package io.kestra.plugin.influxdb;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.junit.annotations.EvaluateTrigger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class FluxTriggerTest {

    @Test
    @EvaluateTrigger(flow = "flows/influx-listen.yml", triggerId = "watch")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        List<Map<String, Object>> rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");

        Map<String, Object> firstRow = rows.getFirst();

        assertThat(firstRow.get("_value"), is(0.48));
        assertThat(rows.getFirst().containsKey("_value"), is(true));
        assertThat(rows.getFirst().containsKey("_field"), is(true));
        assertThat(rows.getFirst().containsKey("_measurement"), is(true));
    }
}
