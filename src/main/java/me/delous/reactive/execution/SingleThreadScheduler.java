package me.delous.reactive.execution;

import java.util.concurrent.Executors;

public class SingleThreadScheduler extends ExecutionContext {
    public SingleThreadScheduler() {
        super(Executors.newSingleThreadExecutor(namedThreadFactory("reactive-serial-worker-")));
    }
}
