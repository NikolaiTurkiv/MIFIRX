package org.example.mifi;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import org.example.mifi.scheduler.MIFIScheduler;

public class MIFIObservable<T> {

    private final MIFIOnSubscribe<T> onSubscribe;

    private MIFIObservable(MIFIOnSubscribe<T> onSubscribe) {
        this.onSubscribe = Objects.requireNonNull(onSubscribe, "onSubscribe must not be null");
    }

    public static <T> MIFIObservable<T> create(MIFIOnSubscribe<T> onSubscribe) {
        return new MIFIObservable<>(onSubscribe);
    }

    public MIFIDisposable subscribe(MIFIObserver<? super T> observer) {
        Objects.requireNonNull(observer, "observer must not be null");

        MIFICreateEmitter<T> emitter = new MIFICreateEmitter<>(observer);

        try {
            onSubscribe.subscribe(emitter);
        } catch (Throwable throwable) {
            emitter.onError(throwable);
        }

        return emitter;
    }

    public <R> MIFIObservable<R> map(Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper must not be null");

        return create(emitter -> {
            MIFIDisposable upstreamDisposable = MIFIObservable.this.subscribe(new MIFIObserver<>() {
            @Override
            public void onNext(T item) {
                if (emitter.isDisposed()) {
                    return;
                }

                try {
                    R mappedItem = mapper.apply(item);
                    emitter.onNext(mappedItem);
                } catch (Throwable throwable) {
                    emitter.onError(throwable);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
            });

            emitter.setDisposable(upstreamDisposable);
        });
    }

    public MIFIObservable<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");

        return create(emitter -> {
            MIFIDisposable upstreamDisposable = MIFIObservable.this.subscribe(new MIFIObserver<>() {
            @Override
            public void onNext(T item) {
                if (emitter.isDisposed()) {
                    return;
                }

                try {
                    if (predicate.test(item)) {
                        emitter.onNext(item);
                    }
                } catch (Throwable throwable) {
                    emitter.onError(throwable);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
            });

            emitter.setDisposable(upstreamDisposable);
        });
    }

    public <R> MIFIObservable<R> flatMap(Function<? super T, MIFIObservable<R>> mapper) {
        Objects.requireNonNull(mapper, "mapper must not be null");

        return create(emitter -> {
            MIFICompositeDisposable compositeDisposable = new MIFICompositeDisposable();
            AtomicBoolean terminated = new AtomicBoolean(false);
            AtomicInteger activeSubscriptions = new AtomicInteger(1);

            emitter.setDisposable(compositeDisposable);

            MIFIDisposable upstreamDisposable = MIFIObservable.this.subscribe(new MIFIObserver<>() {
                @Override
                public void onNext(T item) {
                    if (emitter.isDisposed() || terminated.get()) {
                        return;
                    }

                    MIFIObservable<R> innerObservable;
                    try {
                        innerObservable = Objects.requireNonNull(
                                mapper.apply(item),
                                "flatMap mapper returned null"
                        );
                    } catch (Throwable throwable) {
                        signalError(emitter, compositeDisposable, terminated, throwable);
                        return;
                    }

                    activeSubscriptions.incrementAndGet();

                    MIFIDisposable innerDisposable = innerObservable.subscribe(new MIFIObserver<R>() {
                        @Override
                        public void onNext(R innerItem) {
                            if (!emitter.isDisposed() && !terminated.get()) {
                                emitter.onNext(innerItem);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            signalError(emitter, compositeDisposable, terminated, throwable);
                        }

                        @Override
                        public void onComplete() {
                            signalCompletionIfReady(emitter, terminated, activeSubscriptions.decrementAndGet());
                        }
                    });

                    compositeDisposable.add(innerDisposable);
                }

                @Override
                public void onError(Throwable throwable) {
                    signalError(emitter, compositeDisposable, terminated, throwable);
                }

                @Override
                public void onComplete() {
                    signalCompletionIfReady(emitter, terminated, activeSubscriptions.decrementAndGet());
                }
            });

            compositeDisposable.add(upstreamDisposable);
        });
    }

    public MIFIObservable<T> subscribeOn(MIFIScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler must not be null");

        return create(emitter -> scheduler.execute(() -> {
            if (emitter.isDisposed()) {
                return;
            }

            try {
                MIFIDisposable upstreamDisposable = MIFIObservable.this.subscribe(new MIFIObserver<>() {
                    @Override
                    public void onNext(T item) {
                        emitter.onNext(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        emitter.onError(throwable);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                });

                emitter.setDisposable(upstreamDisposable);
            } catch (Throwable throwable) {
                emitter.onError(throwable);
            }
        }));
    }

    public MIFIObservable<T> observeOn(MIFIScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler must not be null");

        return create(emitter -> {
            MIFIDisposable upstreamDisposable = MIFIObservable.this.subscribe(new MIFIObserver<>() {
                @Override
                public void onNext(T item) {
                    scheduleSafely(scheduler, () -> {
                        if (!emitter.isDisposed()) {
                            emitter.onNext(item);
                        }
                    }, emitter);
                }

                @Override
                public void onError(Throwable throwable) {
                    scheduleSafely(scheduler, () -> emitter.onError(throwable), emitter);
                }

                @Override
                public void onComplete() {
                    scheduleSafely(scheduler, emitter::onComplete, emitter);
                }
            });

            emitter.setDisposable(upstreamDisposable);
        });
    }

    @FunctionalInterface
    public interface MIFIOnSubscribe<T> {
        void subscribe(MIFIEmitter<T> emitter) throws Exception;
    }

    private static void scheduleSafely(MIFIScheduler scheduler, Runnable task, MIFIEmitter<?> emitter) {
        try {
            scheduler.execute(task);
        } catch (Throwable throwable) {
            emitter.onError(throwable);
        }
    }

    private static void signalError(
            MIFIEmitter<?> emitter,
            MIFICompositeDisposable compositeDisposable,
            AtomicBoolean terminated,
            Throwable throwable
    ) {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }

        compositeDisposable.dispose();
        emitter.onError(throwable);
    }

    private static void signalCompletionIfReady(
            MIFIEmitter<?> emitter,
            AtomicBoolean terminated,
            int remainingSubscriptions
    ) {
        if (remainingSubscriptions == 0 && terminated.compareAndSet(false, true)) {
            emitter.onComplete();
        }
    }

    private static final class MIFICreateEmitter<T> implements MIFIEmitter<T> {
        private static final MIFIDisposable DISPOSED = new MIFIDisposable() {
            @Override
            public void dispose() {
            }

            @Override
            public boolean isDisposed() {
                return true;
            }
        };

        private final MIFIObserver<? super T> observer;
        private final AtomicBoolean disposed = new AtomicBoolean(false);
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final AtomicReference<MIFIDisposable> upstreamDisposable = new AtomicReference<>();

        private MIFICreateEmitter(MIFIObserver<? super T> observer) {
            this.observer = observer;
        }

        @Override
        public void onNext(T item) {
            if (isDisposed() || terminated.get()) {
                return;
            }

            observer.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            Throwable actualThrowable = throwable == null
                    ? new NullPointerException("throwable must not be null")
                    : throwable;

            if (isDisposed() || !terminated.compareAndSet(false, true)) {
                return;
            }

            try {
                observer.onError(actualThrowable);
            } finally {
                dispose();
            }
        }

        @Override
        public void onComplete() {
            if (isDisposed() || !terminated.compareAndSet(false, true)) {
                return;
            }

            try {
                observer.onComplete();
            } finally {
                dispose();
            }
        }

        @Override
        public void dispose() {
            if (!disposed.compareAndSet(false, true)) {
                return;
            }

            MIFIDisposable disposable = upstreamDisposable.getAndSet(DISPOSED);
            if (disposable != null && disposable != DISPOSED) {
                disposable.dispose();
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }

        @Override
        public void setDisposable(MIFIDisposable disposable) {
            if (disposable == null) {
                return;
            }

            if (!upstreamDisposable.compareAndSet(null, disposable)) {
                disposable.dispose();
                return;
            }

            if (isDisposed()) {
                MIFIDisposable actualDisposable = upstreamDisposable.getAndSet(DISPOSED);
                if (actualDisposable != null && actualDisposable != DISPOSED) {
                    actualDisposable.dispose();
                }
            }
        }
    }
}
