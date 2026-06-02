package org.example;

import java.util.concurrent.CountDownLatch;

import org.example.mifi.MIFIObservable;
import org.example.mifi.MIFIObserver;
import org.example.mifi.scheduler.MIFIIOThreadScheduler;
import org.example.mifi.scheduler.MIFISingleThreadScheduler;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        MIFIObservable<Integer> observable = MIFIObservable.create(emitter -> {
            System.out.println("Subscribe thread: " + Thread.currentThread().getName());

            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onNext(4);
            emitter.onComplete();
        });

        observable
                .map(item -> item * 10)
                .filter(item -> item >= 20 && item < 40)
                .flatMap(item -> MIFIObservable.<String>create(emitter -> {
                    emitter.onNext("value=" + item);
                    emitter.onNext("double=" + (item * 2));
                    emitter.onComplete();
                }))
                .subscribeOn(new MIFIIOThreadScheduler())
                .observeOn(new MIFISingleThreadScheduler())
                .subscribe(new MIFIObserver<String>() {
            @Override
            public void onNext(String item) {
                System.out.println("Received: " + item + " on " + Thread.currentThread().getName());
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("Error: " + throwable.getMessage());
                latch.countDown();
            }

            @Override
            public void onComplete() {
                System.out.println("Stream completed on " + Thread.currentThread().getName());
                latch.countDown();
            }
        });

        latch.await();
    }
}
