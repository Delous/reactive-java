package me.delous.reactive.stream;

import me.delous.reactive.api.Disposable;
import me.delous.reactive.api.Observer;
import me.delous.reactive.api.Scheduler;
import me.delous.reactive.operators.OperatorSupport;
import me.delous.reactive.signal.Signal;
import me.delous.reactive.signal.SignalType;
import me.delous.reactive.subscription.Subscription;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

public final class ReactiveStream<T> {
    private static final Logger LOGGER = Logger.getLogger(ReactiveStream.class.getName());

    private final StreamSource<T> source;
    private final Scheduler subscribeScheduler;
    private final Scheduler observeScheduler;

    public ReactiveStream(StreamSource<T> source) {
        this(source, null, null);
    }

    private ReactiveStream(StreamSource<T> source, Scheduler subscribeScheduler, Scheduler observeScheduler) {
        this.source = Objects.requireNonNull(source, "source");
        this.subscribeScheduler = subscribeScheduler;
        this.observeScheduler = observeScheduler;
    }

    public Disposable subscribe(Observer<T> observer) {
        Objects.requireNonNull(observer, "observer");
        LOGGER.info(() -> "event=stream.subscribe thread=" + Thread.currentThread().getName());
        Subscription subscription = new Subscription();
        Observer<T> scheduledObserver = observeScheduler == null ? observer : scheduledObserver(observer, observeScheduler, subscription);
        Subscriber<T> subscriber = new Subscriber<>(scheduledObserver, subscription);
        Runnable start = () -> {
            if (subscription.isCancelled()) {
                return;
            }
            try {
                source.start(subscriber);
            } catch (Throwable error) {
                subscriber.fail(error);
            }
        };
        if (subscribeScheduler == null) {
            start.run();
        } else {
            subscribeScheduler.execute(start);
        }
        return subscription;
    }

    public <R> ReactiveStream<R> map(Function<T, R> transform) {
        Objects.requireNonNull(transform, "transform");
        return new ReactiveStream<>(emitter -> {
            AtomicBoolean terminated = new AtomicBoolean(false);
            Disposable upstream = subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                if (emitter.isCancelled() || terminated.get()) {
                    return;
                }
                try {
                    emitter.next(transform.apply(item));
                } catch (Throwable error) {
                    if (terminated.compareAndSet(false, true)) {
                        emitter.fail(error);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                if (terminated.compareAndSet(false, true)) {
                    emitter.fail(t);
                }
            }

            @Override
            public void onComplete() {
                if (terminated.compareAndSet(false, true)) {
                    emitter.done();
                }
            }
        });
            if (emitter instanceof Subscriber<?> subscriber) {
                subscriber.cancelWith(upstream);
            }
        });
    }

    public ReactiveStream<T> filter(Predicate<T> condition) {
        Objects.requireNonNull(condition, "condition");
        return new ReactiveStream<>(emitter -> {
            AtomicBoolean terminated = new AtomicBoolean(false);
            Disposable upstream = subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                if (emitter.isCancelled() || terminated.get()) {
                    return;
                }
                try {
                    if (condition.test(item)) {
                        emitter.next(item);
                    }
                } catch (Throwable error) {
                    if (terminated.compareAndSet(false, true)) {
                        emitter.fail(error);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                if (terminated.compareAndSet(false, true)) {
                    emitter.fail(t);
                }
            }

            @Override
            public void onComplete() {
                if (terminated.compareAndSet(false, true)) {
                    emitter.done();
                }
            }
        });
            if (emitter instanceof Subscriber<?> subscriber) {
                subscriber.cancelWith(upstream);
            }
        });
    }

    public <R> ReactiveStream<R> flatMap(Function<T, me.delous.reactive.api.Observable<R>> expand) {
        Objects.requireNonNull(expand, "expand");
        return new ReactiveStream<>(emitter -> OperatorSupport.flatMap(this, expand, emitter));
    }

    public ReactiveStream<T> subscribeOn(Scheduler scheduler) {
        return new ReactiveStream<>(source, Objects.requireNonNull(scheduler, "scheduler"), observeScheduler);
    }

    public ReactiveStream<T> observeOn(Scheduler scheduler) {
        return new ReactiveStream<>(source, subscribeScheduler, Objects.requireNonNull(scheduler, "scheduler"));
    }

    private static <T> Observer<T> scheduledObserver(Observer<T> observer, Scheduler scheduler, Subscription subscription) {
        return new Observer<>() {
            @Override
            public void onNext(T item) {
                scheduler.execute(() -> {
                    if (!subscription.isCancelled()) {
                        deliver(Signal.next(item), observer);
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                scheduler.execute(() -> {
                    if (!subscription.isCancelled()) {
                        deliver(Signal.error(t), observer);
                    }
                });
            }

            @Override
            public void onComplete() {
                scheduler.execute(() -> {
                    if (!subscription.isCancelled()) {
                        deliver(Signal.complete(), observer);
                    }
                });
            }
        };
    }

    public static <T> void deliver(Signal<T> signal, Observer<T> observer) {
        switch (signal.type()) {
            case NEXT -> {
                LOGGER.info(() -> "event=signal.next value=" + signal.value() + " thread=" + Thread.currentThread().getName());
                observer.onNext(signal.value());
            }
            case ERROR -> {
                LOGGER.info(() -> "event=signal.error type=" + signal.error().getClass().getSimpleName());
                observer.onError(signal.error());
            }
            case COMPLETE -> observer.onComplete();
        }
    }
}
