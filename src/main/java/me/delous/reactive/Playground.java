package me.delous.reactive;

import me.delous.reactive.api.Disposable;
import me.delous.reactive.api.Observable;
import me.delous.reactive.examples.PrintSubscriber;
import me.delous.reactive.execution.IOThreadScheduler;
import me.delous.reactive.execution.SingleThreadScheduler;

public class Playground {
    public static void main(String[] args) throws InterruptedException {
        try (IOThreadScheduler io = new IOThreadScheduler();
             SingleThreadScheduler serial = new SingleThreadScheduler()) {

            Observable<String> words = Observable.create(emitter -> {
                for (String word : new String[]{"alpha", "beta", "gamma", "delta"}) {
                    if (emitter.isCancelled()) {
                        return;
                    }
                    emitter.next(word);
                }
                emitter.done();
            });

            words
                    .map(String::toUpperCase)
                    .filter(word -> word.length() > 4)
                    .flatMap(word -> Observable.create(emitter -> {
                        emitter.next(word + ":first");
                        emitter.next(word + ":second");
                        emitter.done();
                    }))
                    .subscribeOn(io)
                    .observeOn(serial)
                    .subscribe(new PrintSubscriber<>("pipeline"));

            Observable<String> failing = Observable.create(emitter -> {
                emitter.next("safe");
                emitter.next("boom");
                emitter.done();
            });

            failing
                    .map(value -> {
                        if ("boom".equals(value)) {
                            throw new IllegalArgumentException("demo failure");
                        }
                        return value;
                    })
                    .subscribe(new PrintSubscriber<>("errors"));

            Observable<String> cancellable = Observable.create(emitter -> {
                for (String word : new String[]{"red", "green", "blue", "white"}) {
                    if (emitter.isCancelled()) {
                        return;
                    }
                    emitter.next(word);
                    sleep(75);
                }
                emitter.done();
            });

            Disposable disposable = cancellable.subscribeOn(io).subscribe(new PrintSubscriber<>("cancel"));
            sleep(120);
            disposable.dispose();
            sleep(300);
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
