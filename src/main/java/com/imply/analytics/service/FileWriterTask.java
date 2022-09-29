package com.imply.analytics.service;

import com.imply.analytics.model.IPartitioner;
import com.imply.analytics.util.ThreadUtils;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileWriterTask {

    private final String storagePath;
    ExecutorService writerExecutorService;
    Integer partitionCount;
    List<BlockingQueue<String>> sharedQueueList;
    IPartitioner<String>partitioner;


    public FileWriterTask(String storagePath, Integer partitionCount, IPartitioner<String> partitioner) {
        this.storagePath = storagePath;
        this.partitionCount = partitionCount;
        writerExecutorService = Executors.newFixedThreadPool(partitionCount);
        sharedQueueList = new ArrayList<>(partitionCount);
        this.partitioner = partitioner;
        cleanUp(storagePath);

        for(int index = 0; index < partitionCount; index++){
            BlockingQueue<String> sharedQueue = new ArrayBlockingQueue<String>(1000);
            sharedQueueList.add(sharedQueue);
            this.writerExecutorService.execute(new LineConsumer<String>(sharedQueue, storagePath+"/"+index, 10));
        }

    }

    private void cleanUp(String path){
        File folder = new File(path);
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    cleanUp(f.getPath());
                } else {
                    f.delete();
                }
            }
        }
    }

    public void produce(String line) throws InterruptedException {
        int partitionIndex = partitioner.partition(line);
        sharedQueueList.get(partitionIndex).put(line);
    }


    public void shutdown(){
        ThreadUtils.shutdownAndAwaitTermination(this.writerExecutorService);
    }


}
