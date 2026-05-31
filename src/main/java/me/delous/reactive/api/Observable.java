package me.delous.reactive.api;

import me.delous.reactive.stream.ReactiveStream;
import me.delous.reactive.stream.StreamSource;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public final class Observable<T> {
    private final ReactiveStream<T> stream;

    private Observable(ReactiveStream<T> stream) {
        this.stream = Objects.requireNonNull(stream, "stream");
    }

    public static <T> Observable<T> create(StreamSource<T> source) {
        return new Observable<>(new ReactiveStream<>(source));
    }

    public Disposable subscribe(Observer<T> observer) {
        return stream.subscribe(observer);
    }

    public <R> Observable<R> map(Function<T, R> transform) {
        return new Observable<>(stream.map(transform));
    }

    public Observable<T> filter(Predicate<T> condition) {
        return new Observable<>(stream.filter(condition));
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> expand) {
        return new Observable<>(stream.flatMap(expand));
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(stream.subscribeOn(scheduler));
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(stream.observeOn(scheduler));
    }
}
