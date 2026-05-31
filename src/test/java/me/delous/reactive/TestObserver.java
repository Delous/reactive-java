package me.delous.reactive;

import me.delous.reactive.api.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class TestObserver<T> implements Observer<T> {
    private final List<T> values = new ArrayList<>();
    private final CountDownLatch terminalLatch = new CountDownLatch(1);
    private Throwable error;
    private boolean completed;

    @Override
    public synchronized void onNext(T item) {
        values.add(item);
    }

    @Override
    public synchronized void onError(Throwable t) {
        error = t;
        terminalLatch.countDown();
    }

    @Override
    public synchronized void onComplete() {
        completed = true;
        terminalLatch.countDown();
    }

    synchronized List<T> values() {
        return List.copyOf(values);
    }

    synchronized Throwable error() {
        return error;
    }

    synchronized boolean completed() {
        return completed;
    }

    boolean awaitTerminal() throws InterruptedException {
        return terminalLatch.await(2, TimeUnit.SECONDS);
    }
}
