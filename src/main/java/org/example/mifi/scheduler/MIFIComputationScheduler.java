package org.example.mifi.scheduler;

import java.util.concurrent.Executors;

public class MIFIComputationScheduler extends MIFIExecutorScheduler {

    public MIFIComputationScheduler() {
        super(Executors.newFixedThreadPool(
                Math.max(1, Runtime.getRuntime().availableProcessors()),
                createThreadFactory("mifi-computation")
        ));
    }
}
