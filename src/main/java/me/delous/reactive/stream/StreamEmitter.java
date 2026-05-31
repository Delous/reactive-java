package me.delous.reactive.stream;

public interface StreamEmitter<T> {
    void next(T value);

    void fail(Throwable error);

    void done();

    boolean isCancelled();
}
