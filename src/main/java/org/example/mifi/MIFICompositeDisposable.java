package org.example.mifi;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class MIFICompositeDisposable implements MIFIDisposable {

    private final List<MIFIDisposable> disposables = new CopyOnWriteArrayList<>();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    public void add(MIFIDisposable disposable) {
        if (disposable == null) {
            return;
        }

        if (isDisposed()) {
            disposable.dispose();
            return;
        }

        disposables.add(disposable);

        if (isDisposed() && disposables.remove(disposable)) {
            disposable.dispose();
        }
    }

    @Override
    public void dispose() {
        if (!disposed.compareAndSet(false, true)) {
            return;
        }

        for (MIFIDisposable disposable : disposables) {
            disposable.dispose();
        }

        disposables.clear();
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
