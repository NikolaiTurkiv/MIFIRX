package org.example.mifi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.example.mifi.scheduler.MIFIIOThreadScheduler;
import org.example.mifi.scheduler.MIFISingleThreadScheduler;
import org.junit.jupiter.api.Test;

class MIFIObservableTest {

    @Test
    void createShouldEmitItemsAndComplete() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(1, 2, 3), observer.values);
        assertTrue(observer.completed);
        assertNull(observer.error.get());
    }

    @Test
    void createShouldForwardSourceExceptionToOnError() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            throw new IllegalStateException("source failure");
        }).subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(), observer.values);
        assertFalse(observer.completed);
        assertNotNull(observer.error.get());
        assertEquals("source failure", observer.error.get().getMessage());
    }

    @Test
    void mapShouldTransformValues() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).map(item -> item * 10)
                .subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(10, 20, 30), observer.values);
        assertTrue(observer.completed);
        assertNull(observer.error.get());
    }

    @Test
    void mapShouldForwardMapperError() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        }).<Integer>map(item -> {
            throw new IllegalArgumentException("bad map");
        }).subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(), observer.values);
        assertFalse(observer.completed);
        assertNotNull(observer.error.get());
        assertEquals("bad map", observer.error.get().getMessage());
    }

    @Test
    void filterShouldKeepOnlyMatchingValues() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onNext(4);
            emitter.onComplete();
        }).filter(item -> item % 2 == 0)
                .subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(2, 4), observer.values);
        assertTrue(observer.completed);
        assertNull(observer.error.get());
    }

    @Test
    void flatMapShouldMergeInnerObservables() throws InterruptedException {
        RecordingObserver<String> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).flatMap(item -> MIFIObservable.<String>create(emitter -> {
            emitter.onNext("value=" + item);
            emitter.onNext("square=" + (item * item));
            emitter.onComplete();
        })).subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of("value=2", "square=4", "value=3", "square=9"), observer.values);
        assertTrue(observer.completed);
        assertNull(observer.error.get());
    }

    @Test
    void flatMapShouldForwardInnerError() throws InterruptedException {
        RecordingObserver<String> observer = new RecordingObserver<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).flatMap(item -> MIFIObservable.<String>create(emitter -> {
            if (item == 2) {
                emitter.onError(new IllegalStateException("inner failure"));
                return;
            }

            emitter.onNext("ok=" + item);
            emitter.onComplete();
        })).subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of("ok=1"), observer.values);
        assertFalse(observer.completed);
        assertNotNull(observer.error.get());
        assertEquals("inner failure", observer.error.get().getMessage());
    }

    @Test
    void disposeShouldStopFurtherEmissions() throws Exception {
        RecordingObserver<Integer> observer = new RecordingObserver<>();
        CountDownLatch startEmission = new CountDownLatch(1);
        CountDownLatch workerFinished = new CountDownLatch(1);
        AtomicReference<MIFIDisposable> disposableRef = new AtomicReference<>();

        MIFIObservable<Integer> observable = MIFIObservable.create(emitter -> {
            AtomicBoolean cancelled = new AtomicBoolean(false);

            emitter.setDisposable(new MIFIDisposable() {
                @Override
                public void dispose() {
                    cancelled.set(true);
                }

                @Override
                public boolean isDisposed() {
                    return cancelled.get();
                }
            });

            Thread worker = new Thread(() -> {
                try {
                    startEmission.await();

                    for (int i = 1; i <= 5; i++) {
                        if (cancelled.get()) {
                            return;
                        }

                        emitter.onNext(i);
                        Thread.sleep(30);
                    }

                    emitter.onComplete();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    workerFinished.countDown();
                }
            });

            worker.start();
        });

        MIFIDisposable disposable = observable.subscribe(new MIFIObserver<>() {
            @Override
            public void onNext(Integer item) {
                observer.onNext(item);

                if (item == 2) {
                    disposableRef.get().dispose();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                observer.onError(throwable);
            }

            @Override
            public void onComplete() {
                observer.onComplete();
            }
        });

        disposableRef.set(disposable);
        startEmission.countDown();

        assertTrue(workerFinished.await(2, TimeUnit.SECONDS));
        Thread.sleep(100);

        assertEquals(List.of(1, 2), observer.values);
        assertFalse(observer.completed);
        assertNull(observer.error.get());
        assertTrue(disposable.isDisposed());
    }

    @Test
    void subscribeOnShouldRunSourceOnSchedulerThread() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();
        String testThreadName = Thread.currentThread().getName();
        AtomicReference<String> sourceThreadName = new AtomicReference<>();

        MIFIObservable.<Integer>create(emitter -> {
            sourceThreadName.set(Thread.currentThread().getName());
            emitter.onNext(1);
            emitter.onComplete();
        }).subscribeOn(new MIFISingleThreadScheduler())
                .subscribe(observer);

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(1), observer.values);
        assertNotNull(sourceThreadName.get());
        assertTrue(sourceThreadName.get().startsWith("mifi-single-"));
        assertFalse(testThreadName.equals(sourceThreadName.get()));
    }

    @Test
    void observeOnShouldRunObserverOnSchedulerThread() throws InterruptedException {
        RecordingObserver<Integer> observer = new RecordingObserver<>();
        String sourceThreadName = Thread.currentThread().getName();
        AtomicReference<String> observerThreadName = new AtomicReference<>();

        MIFIObservable.<Integer>create(emitter -> {
            emitter.onNext(10);
            emitter.onComplete();
        }).observeOn(new MIFIIOThreadScheduler())
                .subscribe(new MIFIObserver<>() {
                    @Override
                    public void onNext(Integer item) {
                        observerThreadName.set(Thread.currentThread().getName());
                        observer.onNext(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        observer.onError(throwable);
                    }

                    @Override
                    public void onComplete() {
                        observer.onComplete();
                    }
                });

        assertTrue(observer.awaitTerminalEvent());
        assertEquals(List.of(10), observer.values);
        assertNotNull(observerThreadName.get());
        assertTrue(observerThreadName.get().startsWith("mifi-io-"));
        assertFalse(sourceThreadName.equals(observerThreadName.get()));
    }

    private static class RecordingObserver<T> implements MIFIObserver<T> {
        private final List<T> values = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch terminalEvent = new CountDownLatch(1);
        private volatile boolean completed;

        @Override
        public void onNext(T item) {
            values.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            error.set(throwable);
            terminalEvent.countDown();
        }

        @Override
        public void onComplete() {
            completed = true;
            terminalEvent.countDown();
        }

        private boolean awaitTerminalEvent() throws InterruptedException {
            return terminalEvent.await(2, TimeUnit.SECONDS);
        }
    }
}
