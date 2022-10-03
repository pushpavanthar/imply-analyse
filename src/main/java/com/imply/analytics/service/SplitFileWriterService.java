package com.imply.analytics.service;

import com.imply.analytics.model.IPartitioner;
import com.imply.analytics.util.IConstants;
import com.imply.analytics.util.ThreadUtils;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class SplitFileWriterService implements IService{

    private final String storagePath;
    ExecutorService writerExecutorService;
    Integer partitionCount;
    List<BlockingQueue<String>> sharedQueueList;
    IPartitioner<String>partitioner;
    CyclicBarrier cyclicBarrier;
    IService nextService;


    public SplitFileWriterService(ExecutorService sharedExecutorService, String storagePath, Integer partitionCount, IPartitioner<String> partitioner, IService nextServie) {
        this.storagePath = storagePath;
        this.partitionCount = partitionCount;
        this.writerExecutorService = sharedExecutorService;
        sharedQueueList = Collections.synchronizedList(new ArrayList<>(partitionCount));
        this.partitioner = partitioner;
        this.nextService = nextServie;

    }

    public void produce(String line) throws InterruptedException {
        if(!line.equals(IConstants.DUMMY)) {
            int partitionIndex = partitioner.partition(line);
            sharedQueueList.get(partitionIndex).put(line);
        } else {
            for(int partition = 0; partition < partitionCount; partition++) {
                sharedQueueList.get(partition).put(line);
            }
        }
    }


    @Override
    public void initialize() {
        ThreadUtils.cleanUp(storagePath);
        cyclicBarrier = new CyclicBarrier(partitionCount,new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                System.out.println("Shutting Down Writer Executor Service");
                nextService.start();
            }
        });
    }

    @Override
    public void start() {
        initialize();
        for(int index = 0; index < partitionCount; index++){
            BlockingQueue<String> sharedQueue = new ArrayBlockingQueue<String>(1000);
            sharedQueueList.add(sharedQueue);
            this.writerExecutorService.execute(new LineConsumer<String>(sharedQueue, storagePath+"/"+index, 10, partitionCount, cyclicBarrier));
        }
    }

    public void shutdown(){
//        ThreadUtils.shutdownAndAwaitTermination(this.writerExecutorService);
    }


}
