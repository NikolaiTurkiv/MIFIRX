package org.example.mifi.scheduler;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

abstract class MIFIExecutorScheduler implements MIFIScheduler {

    private final ExecutorService executorService;

    protected MIFIExecutorScheduler(ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService, "executorService must not be null");
    }

    @Override
    public void execute(Runnable task) {
        executorService.execute(task);
    }

    protected static ThreadFactory createThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger(1);

        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(prefix + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }
}
