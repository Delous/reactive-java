package me.delous.reactive.api;

public interface Disposable {
    void dispose();

    boolean isDisposed();
}
