package me.delous.reactive;

import me.delous.reactive.api.Disposable;
import me.delous.reactive.api.Observable;
import me.delous.reactive.execution.IOThreadScheduler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionTest {
    @Test
    void disposeStopsFutureEvents() throws InterruptedException {
        try (IOThreadScheduler scheduler = new IOThreadScheduler()) {
            CountDownLatch firstValue = new CountDownLatch(1);
            TestObserver<String> observer = new TestObserver<>() {
                @Override
                public synchronized void onNext(String item) {
                    super.onNext(item);
                    firstValue.countDown();
                }
            };

            Disposable disposable = Observable.<String>create(emitter -> {
                emitter.next("alpha");
                sleep(150);
                if (!emitter.isCancelled()) {
                    emitter.next("beta");
                }
                emitter.done();
            }).subscribeOn(scheduler).subscribe(observer);

            assertThat(firstValue.await(2, TimeUnit.SECONDS)).isTrue();
            disposable.dispose();
            Thread.sleep(250);

            assertThat(disposable.isDisposed()).isTrue();
            assertThat(observer.values()).containsExactly("alpha");
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }
}
