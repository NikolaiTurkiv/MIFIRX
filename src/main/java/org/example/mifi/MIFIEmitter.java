package org.example.mifi;

public interface MIFIEmitter<T> extends MIFIDisposable {
    void onNext(T item);

    void onError(Throwable throwable);

    void onComplete();

    void setDisposable(MIFIDisposable disposable);
}
