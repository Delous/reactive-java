package me.delous.reactive.api;

public interface Observer<T> {
    void onNext(T item);

    void onError(Throwable t);

    void onComplete();
}
