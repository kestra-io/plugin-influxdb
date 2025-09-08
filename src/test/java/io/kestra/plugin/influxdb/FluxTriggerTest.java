package io.kestra.plugin.influxdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class FluxTriggerTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;

    @Test
    void run() throws Exception {
        Execution execution = triggerFlow();

        List<Map<String, Object>> rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");

        Map<String, Object> firstRow = rows.getFirst();

        assertThat(firstRow.get("_value"), is(0.48));
        assertThat(rows.getFirst().containsKey("_value"), is(true));
        assertThat(rows.getFirst().containsKey("_field"), is(true));
        assertThat(rows.getFirst().containsKey("_measurement"), is(true));
    }

    protected Execution triggerFlow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);

        try (
            AbstractScheduler scheduler = new JdbcScheduler(applicationContext, flowListenersService);
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        ) {
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("influx-listen"));
            });

            worker.run();
            scheduler.run();

            localFlowRepositoryLoader.load(
                Objects.requireNonNull(this.getClass().getClassLoader().getResource("flows/influx-listen.yml"))
            );

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            return receive.blockLast();
        }
    }
}
