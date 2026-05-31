package me.delous.reactive.execution;

import me.delous.reactive.api.Scheduler;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ExecutionContext implements Scheduler, AutoCloseable {
    private final ExecutorService executor;

    protected ExecutionContext(ExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    protected static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger(1);
        return task -> {
            Thread thread = new Thread(task, prefix + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }
}
