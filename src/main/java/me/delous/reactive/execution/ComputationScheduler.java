package me.delous.reactive.execution;

import java.util.concurrent.Executors;

public class ComputationScheduler extends ExecutionContext {
    public ComputationScheduler() {
        super(Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                namedThreadFactory("reactive-cpu-worker-")
        ));
    }
}
