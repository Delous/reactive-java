package me.delous.reactive.signal;

import java.util.Objects;

public final class Signal<T> {
    private final SignalType type;
    private final T value;
    private final Throwable error;

    private Signal(SignalType type, T value, Throwable error) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
        this.error = error;
    }

    public static <T> Signal<T> next(T value) {
        return new Signal<>(SignalType.NEXT, value, null);
    }

    public static <T> Signal<T> error(Throwable error) {
        return new Signal<>(SignalType.ERROR, null, Objects.requireNonNull(error, "error"));
    }

    public static <T> Signal<T> complete() {
        return new Signal<>(SignalType.COMPLETE, null, null);
    }

    public SignalType type() {
        return type;
    }

    public T value() {
        return value;
    }

    public Throwable error() {
        return error;
    }
}
