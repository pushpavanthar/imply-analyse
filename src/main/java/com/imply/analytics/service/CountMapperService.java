package com.imply.analytics.service;

import com.imply.analytics.util.ThreadUtils;
import lombok.SneakyThrows;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;

public class CountMapperService implements IService{

    private final String storagePath;
    ExecutorService writerExecutorService;
    Integer partitionCount;
    CyclicBarrier cyclicBarrier;
    CountDownLatch latch;


    public CountMapperService(ExecutorService sharedExecutorService, String storagePath, Integer partitionCount, CountDownLatch latch) {
        this.storagePath = storagePath;
        this.partitionCount = partitionCount;
        writerExecutorService = sharedExecutorService;
        this.latch = latch;
    }

    @Override
    public void initialize() {
        ThreadUtils.cleanUp(storagePath);
        cyclicBarrier = new CyclicBarrier(partitionCount, new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                System.out.println("Shutting Down CountMapper Executor Service");
                writerExecutorService.shutdown();
                latch.countDown();
                System.out.println(System.currentTimeMillis());
            }
        });
    }

    public void start() {
        initialize();
        for(int index = 0; index < partitionCount; index++){
            this.writerExecutorService.execute(new FrequencyCounterMapperTask("src/main/resources/", index, cyclicBarrier));
        }
    }




    public void shutdown(){
        ThreadUtils.shutdownAndAwaitTermination(this.writerExecutorService);
    }


}
