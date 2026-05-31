package me.delous.reactive;

import me.delous.reactive.api.Observable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapOperatorTest {
    @Test
    void expandsOuterValuesIntoInnerStreams() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<String>create(emitter -> {
            emitter.next("alpha");
            emitter.next("beta");
            emitter.done();
        }).flatMap(word -> Observable.<String>create(inner -> {
            inner.next(word + ":left");
            inner.next(word + ":right");
            inner.done();
        })).subscribe(observer);

        assertThat(observer.values()).containsExactly("alpha:left", "alpha:right", "beta:left", "beta:right");
        assertThat(observer.completed()).isTrue();
    }

    @Test
    void completesAfterOuterAndInnerStreamsComplete() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<String>create(emitter -> {
            emitter.next("gamma");
            emitter.done();
        }).flatMap(word -> Observable.<String>create(inner -> {
            inner.next(word + ":inner");
            inner.done();
        })).subscribe(observer);

        assertThat(observer.values()).containsExactly("gamma:inner");
        assertThat(observer.completed()).isTrue();
        assertThat(observer.error()).isNull();
    }
}
