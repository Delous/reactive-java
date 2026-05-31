package me.delous.reactive.stream;

import me.delous.reactive.api.Observer;
import me.delous.reactive.api.Disposable;
import me.delous.reactive.signal.Signal;
import me.delous.reactive.signal.SignalType;
import me.delous.reactive.subscription.Subscription;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Subscriber<T> implements StreamEmitter<T> {
    private final Observer<T> observer;
    private final Subscription subscription;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public Subscriber(Observer<T> observer, Subscription subscription) {
        this.observer = Objects.requireNonNull(observer, "observer");
        this.subscription = Objects.requireNonNull(subscription, "subscription");
    }

    public void accept(Signal<T> signal) {
        if (subscription.isCancelled()) {
            return;
        }
        if (signal.type() != SignalType.NEXT && !terminated.compareAndSet(false, true)) {
            return;
        }
        if (signal.type() == SignalType.NEXT && terminated.get()) {
            return;
        }
        ReactiveStream.deliver(signal, observer);
    }

    @Override
    public void next(T value) {
        accept(Signal.next(value));
    }

    @Override
    public void fail(Throwable error) {
        accept(Signal.error(error));
    }

    @Override
    public void done() {
        accept(Signal.complete());
    }

    @Override
    public boolean isCancelled() {
        return subscription.isCancelled();
    }

    public void cancelWith(Disposable disposable) {
        subscription.addCancelAction(disposable);
    }
}
