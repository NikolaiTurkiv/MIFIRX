package org.example.mifi;

public interface MIFIObserver<T> {
    void onNext(T item);

    void onError(Throwable throwable);

    void onComplete();
}
