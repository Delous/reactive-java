package me.delous.reactive;

import me.delous.reactive.api.Observable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StreamCreationTest {
    @Test
    void subscribesToCreatedStream() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<String>create(emitter -> {
            emitter.next("alpha");
            emitter.next("beta");
            emitter.done();
        }).subscribe(observer);

        assertThat(observer.values()).containsExactly("alpha", "beta");
        assertThat(observer.completed()).isTrue();
        assertThat(observer.error()).isNull();
    }
}
