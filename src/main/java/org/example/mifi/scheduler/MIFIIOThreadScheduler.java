package org.example.mifi.scheduler;

import java.util.concurrent.Executors;

public class MIFIIOThreadScheduler extends MIFIExecutorScheduler {

    public MIFIIOThreadScheduler() {
        super(Executors.newCachedThreadPool(createThreadFactory("mifi-io")));
    }
}
