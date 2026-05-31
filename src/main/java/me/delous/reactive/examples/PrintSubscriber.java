package me.delous.reactive.examples;

import me.delous.reactive.api.Observer;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PrintSubscriber<T> implements Observer<T> {
    private static final Logger LOGGER = Logger.getLogger(PrintSubscriber.class.getName());

    private final String name;

    public PrintSubscriber(String name) {
        this.name = name;
    }

    @Override
    public void onNext(T item) {
        LOGGER.info(() -> "event=example.next subscriber=" + name + " value=" + item + " thread=" + Thread.currentThread().getName());
    }

    @Override
    public void onError(Throwable t) {
        LOGGER.log(Level.INFO, "event=example.error subscriber=" + name + " type=" + t.getClass().getSimpleName(), t);
    }

    @Override
    public void onComplete() {
        LOGGER.info(() -> "event=example.complete subscriber=" + name + " thread=" + Thread.currentThread().getName());
    }
}
