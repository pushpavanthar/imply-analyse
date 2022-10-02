package com.imply.analytics.service;


import com.imply.analytics.util.IConstants;
import lombok.SneakyThrows;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

class LineConsumer<E> implements Runnable{
    private final BlockingQueue<E> sharedQueue;
    private final String fileName;
    private final int batch;
    private final Integer partitions;
    private final AtomicInteger poisonPillCounter;
    private final CyclicBarrier cyclicBarrier;

    public LineConsumer(BlockingQueue<E> sharedQueue, String fileName, Integer batchSize, Integer partitions, CyclicBarrier cyclicBarrier) {
        this.sharedQueue = sharedQueue;
        this.fileName = fileName;
        this.batch = batchSize;
        this.partitions = partitions;
        this.cyclicBarrier = cyclicBarrier;
        poisonPillCounter = new AtomicInteger(0);
    }


    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            List<E> list = new ArrayList<>();
//            while(sharedQueue.size() >= batch) {
                sharedQueue.drainTo(list, batch);
                BufferedWriter bufferedWriter = null;
                try {
                    FileWriter writer = new FileWriter(fileName, true);
                    bufferedWriter = new BufferedWriter(writer);
                    StringBuffer stringBuffer = new StringBuffer();
                    list.forEach(x -> {
                        if(x.equals(IConstants.DUMMY)){
                           poisonPillCounter.incrementAndGet();
                        } else {
                            stringBuffer.append(x).append("\n");
                        }
                    });
                    bufferedWriter.write(stringBuffer.toString());
                    bufferedWriter.close();
                } catch (Exception e) {
//                throw e;
                } finally {
                    if (bufferedWriter != null) {
                        bufferedWriter.close();
                    }
                }
//            }
            if(poisonPillCounter.get() == partitions) {
                cyclicBarrier.await();
                break;
            }
        }
    }

    public void produce(E object) {

    }
}
