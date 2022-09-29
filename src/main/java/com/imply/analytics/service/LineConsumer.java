package com.imply.analytics.service;


import lombok.SneakyThrows;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class LineConsumer<E> implements Runnable{
    private final BlockingQueue<E> sharedQueue;
    private final String fileName;
    private final int batch;

    public LineConsumer(BlockingQueue<E> sharedQueue, String fileName, Integer batchSize) {
        this.sharedQueue = sharedQueue;
        this.fileName = fileName;
        this.batch = batchSize;
    }


    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            List<E> list = new ArrayList<>();
            while(sharedQueue.size() >= batch) {
                sharedQueue.drainTo(list, batch);
                BufferedWriter bufferedWriter = null;
                try {
                    FileWriter writer = new FileWriter(fileName, true);
                    bufferedWriter = new BufferedWriter(writer);
                    StringBuffer stringBuffer = new StringBuffer();
                    list.forEach(x-> stringBuffer.append(x).append("\n"));
                    bufferedWriter.write(stringBuffer.toString());
                    bufferedWriter.close();
                } catch (Exception e) {
//                throw e;
                } finally {
                    if (bufferedWriter != null) {
                        bufferedWriter.close();
                    }
                }
            }
            if()
        }
    }

    public void produce(E object) {

    }
}
