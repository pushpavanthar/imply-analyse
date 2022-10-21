package com.imply.analytics.service;

import com.imply.analytics.util.Util;
import lombok.SneakyThrows;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;

public class CountMapperService implements IService{

    private final String dstLocation;
    private final String srcLocation;
    private final ExecutorService writerExecutorService;
    private final Integer partitionCount;
    private CyclicBarrier cyclicBarrier;
    private final CountDownLatch latch;


    public CountMapperService(ExecutorService sharedExecutorService, String srcLocation, String dstLocation, Integer partitionCount, CountDownLatch latch) {
        this.dstLocation = dstLocation;
        this.srcLocation = srcLocation;
        this.partitionCount = partitionCount;
        writerExecutorService = sharedExecutorService;
        this.latch = latch;
    }

    @Override
    public void initialize() {
        Util.cleanUp(dstLocation);
        cyclicBarrier = new CyclicBarrier(partitionCount, new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                System.out.println("Shutting Down shared Executor Service");
                writerExecutorService.shutdown();
                latch.countDown();
            }
        });
    }

    public void start() {
        initialize();
        for(int index = 0; index < partitionCount; index++){
            this.writerExecutorService.execute(new FrequencyCounterMapperTask(srcLocation, dstLocation, index, cyclicBarrier));
        }
    }


    public void teardown(){
        Util.shutdownAndAwaitTermination(this.writerExecutorService);
    }


}
