package me.delous.reactive.execution;

import java.util.concurrent.Executors;

public class IOThreadScheduler extends ExecutionContext {
    public IOThreadScheduler() {
        super(Executors.newCachedThreadPool(namedThreadFactory("reactive-io-worker-")));
    }
}
