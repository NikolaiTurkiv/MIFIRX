package org.example.mifi.scheduler;

import java.util.concurrent.Executors;

public class MIFISingleThreadScheduler extends MIFIExecutorScheduler {

    public MIFISingleThreadScheduler() {
        super(Executors.newSingleThreadExecutor(createThreadFactory("mifi-single")));
    }
}
