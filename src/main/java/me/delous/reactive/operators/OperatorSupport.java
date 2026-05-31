package me.delous.reactive.operators;

import me.delous.reactive.api.Observable;
import me.delous.reactive.api.Observer;
import me.delous.reactive.stream.ReactiveStream;
import me.delous.reactive.stream.Subscriber;
import me.delous.reactive.stream.StreamEmitter;
import me.delous.reactive.subscription.SubscriptionBag;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class OperatorSupport {
    private OperatorSupport() {
    }

    public static <T, R> void flatMap(
            ReactiveStream<T> upstream,
            Function<T, Observable<R>> expand,
            StreamEmitter<R> emitter
    ) {
        SubscriptionBag bag = new SubscriptionBag();
        if (emitter instanceof Subscriber<?> subscriber) {
            subscriber.cancelWith(bag);
        }
        AtomicInteger active = new AtomicInteger(1);
        AtomicBoolean terminated = new AtomicBoolean(false);

        Runnable tryComplete = () -> {
            if (active.decrementAndGet() == 0 && terminated.compareAndSet(false, true) && !emitter.isCancelled()) {
                emitter.done();
            }
        };

        bag.add(upstream.subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                if (emitter.isCancelled() || terminated.get()) {
                    bag.dispose();
                    return;
                }
                Observable<R> inner;
                try {
                    inner = Objects.requireNonNull(expand.apply(item), "expand returned null");
                } catch (Throwable error) {
                    fail(error);
                    return;
                }
                active.incrementAndGet();
                bag.add(inner.subscribe(new Observer<>() {
                    @Override
                    public void onNext(R item) {
                        if (!emitter.isCancelled() && !terminated.get()) {
                            emitter.next(item);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail(t);
                    }

                    @Override
                    public void onComplete() {
                        tryComplete.run();
                    }
                }));
            }

            @Override
            public void onError(Throwable t) {
                fail(t);
            }

            @Override
            public void onComplete() {
                tryComplete.run();
            }

            private void fail(Throwable error) {
                if (terminated.compareAndSet(false, true) && !emitter.isCancelled()) {
                    emitter.fail(error);
                }
                bag.dispose();
            }
        }));
    }
}
