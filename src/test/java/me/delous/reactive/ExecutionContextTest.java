package me.delous.reactive;

import me.delous.reactive.api.Observable;
import me.delous.reactive.api.Observer;
import me.delous.reactive.execution.IOThreadScheduler;
import me.delous.reactive.execution.SingleThreadScheduler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionContextTest {
    @Test
    void subscribeOnRunsSourceOnSchedulerThread() throws InterruptedException {
        try (IOThreadScheduler scheduler = new IOThreadScheduler()) {
            AtomicReference<String> sourceThread = new AtomicReference<>();
            TestObserver<String> observer = new TestObserver<>();

            Observable.<String>create(emitter -> {
                sourceThread.set(Thread.currentThread().getName());
                emitter.next("alpha");
                emitter.done();
            }).subscribeOn(scheduler).subscribe(observer);

            assertThat(observer.awaitTerminal()).isTrue();
            assertThat(sourceThread.get()).startsWith("reactive-io-worker-");
        }
    }

    @Test
    void observeOnRunsObserverCallbacksOnSchedulerThread() throws InterruptedException {
        try (SingleThreadScheduler scheduler = new SingleThreadScheduler()) {
            CountDownLatch callback = new CountDownLatch(1);
            AtomicReference<String> callbackThread = new AtomicReference<>();

            Observable.<String>create(emitter -> {
                emitter.next("alpha");
                emitter.done();
            }).observeOn(scheduler).subscribe(new Observer<>() {
                @Override
                public void onNext(String item) {
                    callbackThread.set(Thread.currentThread().getName());
                    callback.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    callback.countDown();
                }

                @Override
                public void onComplete() {
                    callback.countDown();
                }
            });

            assertThat(callback.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(callbackThread.get()).startsWith("reactive-serial-worker-");
        }
    }
}
