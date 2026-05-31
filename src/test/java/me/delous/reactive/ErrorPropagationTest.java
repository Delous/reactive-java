package me.delous.reactive;

import me.delous.reactive.api.Observable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorPropagationTest {
    @Test
    void sourceErrorGoesToObserver() {
        TestObserver<String> observer = new TestObserver<>();
        IllegalStateException failure = new IllegalStateException("source");

        Observable.<String>create(emitter -> {
            emitter.next("alpha");
            emitter.fail(failure);
            emitter.next("beta");
            emitter.done();
        }).subscribe(observer);

        assertThat(observer.values()).containsExactly("alpha");
        assertThat(observer.error()).isSameAs(failure);
        assertThat(observer.completed()).isFalse();
    }

    @Test
    void mapErrorTerminatesStream() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<String>create(emitter -> {
            emitter.next("alpha");
            emitter.next("beta");
            emitter.done();
        }).map(value -> {
            if ("beta".equals(value)) {
                throw new IllegalArgumentException("bad value");
            }
            return value.toUpperCase();
        }).subscribe(observer);

        assertThat(observer.values()).containsExactly("ALPHA");
        assertThat(observer.error()).isInstanceOf(IllegalArgumentException.class);
        assertThat(observer.completed()).isFalse();
    }

    @Test
    void filterErrorTerminatesStream() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<String>create(emitter -> {
            emitter.next("alpha");
            emitter.next("beta");
            emitter.done();
        }).filter(value -> {
            if ("beta".equals(value)) {
                throw new IllegalArgumentException("bad predicate");
            }
            return true;
        }).subscribe(observer);

        assertThat(observer.values()).containsExactly("alpha");
        assertThat(observer.error()).isInstanceOf(IllegalArgumentException.class);
        assertThat(observer.completed()).isFalse();
    }

    @Test
    void flatMapExpandErrorTerminatesStream() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<String>create(emitter -> {
            emitter.next("alpha");
            emitter.done();
        }).<String>flatMap(value -> {
            throw new IllegalArgumentException("bad expand");
        }).subscribe(observer);

        assertThat(observer.values()).isEmpty();
        assertThat(observer.error()).isInstanceOf(IllegalArgumentException.class);
        assertThat(observer.completed()).isFalse();
    }
}
