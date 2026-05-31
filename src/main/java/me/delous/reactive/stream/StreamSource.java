package me.delous.reactive.stream;

@FunctionalInterface
public interface StreamSource<T> {
    void start(StreamEmitter<T> emitter);
}
