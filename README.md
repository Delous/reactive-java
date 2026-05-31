# reactive-java

Основная точка входа в библиотеку - `Observable<T>`:

```java
Observable<String> stream = Observable.create(emitter -> {
    emitter.next("alpha");
    emitter.next("beta");
    emitter.done();
});

Disposable disposable = stream
        .map(String::toUpperCase)
        .filter(value -> value.length() > 4)
        .subscribe(observer);
```

Подписчик реализует `Observer<T>`:

```java
public interface Observer<T> {
    void onNext(T item);
    void onError(Throwable t);
    void onComplete();
}
```

`Disposable` нужен для отмены подписки:

```java
disposable.dispose();
boolean cancelled = disposable.isDisposed();
```

## Шаблон Observer

Библиотека использует шаблон Observer: источник публикует события, а подписчик получает вызовы `onNext`, `onError` и `onComplete`. Ошибка и успешное завершение считаются конечными событиями. После них новые значения подписчику не доставляются.

## Observable как фасад

`Observable<T>` - тонкий публичный фасад. Он не хранит основную логику обработки потока, а передает подписку, операторы и переключение потоков во внутренний `ReactiveStream<T>`.

Такой подход отделяет устойчивый публичный API от внутренней композиционной реализации.

## Модель Signal

Внутри все события представлены через `Signal<T>`:

- `SignalType.NEXT` - очередное значение;
- `SignalType.ERROR` - ошибка;
- `SignalType.COMPLETE` - завершение.

Фабричные методы `Signal.next`, `Signal.error` и `Signal.complete` используются для единообразной доставки событий через `ReactiveStream.deliver`.

## Источники и эмиттеры

`StreamSource<T>` запускает пользовательский источник:

```java
void start(StreamEmitter<T> emitter);
```

`StreamEmitter<T>` публикует значения, ошибки и завершение, а также сообщает об отмене через `isCancelled()`.

## Disposable, Subscription и SubscriptionBag

`Subscription` реализует `Disposable` поверх `AtomicBoolean cancelled`. После вызова `dispose()` новые события не попадают в `Observer`.

`SubscriptionBag` хранит несколько `Disposable` и отменяет их все одним вызовом `dispose()`. Он используется в `flatMap`, чтобы отмена внешней подписки отменяла активные внутренние подписки.

## Операторы

`map` применяет `Function<T, R>` к каждому элементу. Исключение из функции передается в `onError`.

`filter` пропускает только элементы, для которых `Predicate<T>` вернул `true`. Исключение из условия передается в `onError`.

`flatMap` превращает каждый элемент внешнего потока во внутренний `Observable<R>` и передает элементы внутренних потоков дальше. `onComplete` вызывается после завершения внешнего потока и всех активных внутренних потоков.

## Планировщики

`Scheduler` имеет один метод:

```java
void execute(Runnable task);
```

Реализованы контексты выполнения:

- `IOThreadScheduler` - `Executors.newCachedThreadPool`, потоки `reactive-io-worker-N`;
- `ComputationScheduler` - пул фиксированного размера по числу процессоров, потоки `reactive-cpu-worker-N`;
- `SingleThreadScheduler` - исполнитель с одним потоком, поток `reactive-serial-worker-N`.

Все они наследуются от `ExecutionContext`, который реализует `Scheduler` и `AutoCloseable`.

## subscribeOn и observeOn

`subscribeOn(scheduler)` переносит запуск `source.start(emitter)` на указанный `Scheduler`.

`observeOn(scheduler)` переносит доставку сигналов в методы `Observer` на указанный `Scheduler`. Доставка проходит через единый метод `deliver(Signal<T> signal, Observer<T> observer)`.

## Логирование

Для логирования используется `java.util.logging.Logger`. Основные сообщения имеют такой формат:

```text
event=stream.subscribe thread=main
event=signal.next value=alpha thread=reactive-serial-worker-1
event=signal.error type=IllegalArgumentException
event=subscription.cancelled
```

## Пример flatMap

```java
Observable.create(emitter -> {
    emitter.next("alpha");
    emitter.done();
})
.flatMap(word -> Observable.create(inner -> {
    inner.next(word + ":first");
    inner.next(word + ":second");
    inner.done();
}))
.subscribe(observer);
```

## Примеры использования

Библиотека может использоваться для обработки последовательностей данных и событий, особенно когда данные поступают постепенно, требуют преобразования, фильтрации, асинхронной обработки или реакции на ошибки. Она подходит не только для событий пользовательского интерфейса, сетевых запросов, чтения файлов, обработки сообщений и фоновых задач.
