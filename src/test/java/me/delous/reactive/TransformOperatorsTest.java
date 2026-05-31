package me.delous.reactive;

import me.delous.reactive.api.Observable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TransformOperatorsTest {
    @Test
    void mapsValues() {
        TestObserver<Integer> observer = new TestObserver<>();

        words().map(String::length).subscribe(observer);

        assertThat(observer.values()).containsExactly(5, 4, 5);
        assertThat(observer.completed()).isTrue();
    }

    @Test
    void filtersValues() {
        TestObserver<String> observer = new TestObserver<>();

        words().filter(value -> value.length() == 5).subscribe(observer);

        assertThat(observer.values()).containsExactly("alpha", "gamma");
        assertThat(observer.completed()).isTrue();
    }

    private Observable<String> words() {
        return Observable.create(emitter -> {
            emitter.next("alpha");
            emitter.next("beta");
            emitter.next("gamma");
            emitter.done();
        });
    }
}
