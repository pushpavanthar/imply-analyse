package com.imply.analytics.service;

import com.imply.analytics.util.ThreadUtils;

import java.io.File;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CountMapper {

    private final String storagePath;
    ExecutorService writerExecutorService;
    Integer partitionCount;
    CyclicBarrier cyclicBarrier;


    public CountMapper(String storagePath, Integer partitionCount) {
        this.storagePath = storagePath;
        this.partitionCount = partitionCount;
        writerExecutorService = Executors.newFixedThreadPool(partitionCount);
        cleanUp(storagePath);
        cyclicBarrier = new CyclicBarrier(partitionCount, new Runnable() {
            @Override
            public void run() {
                System.out.println("Shutting Down CountMapper Executor Service");
                writerExecutorService.shutdown();
                System.out.println(System.currentTimeMillis());
            }
        });
    }

    public void start() {
        for(int index = 0; index < partitionCount; index++){
            this.writerExecutorService.execute(new FrequencyCounterMapperTask("src/main/resources/", index, cyclicBarrier));
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



    public void shutdown(){
        ThreadUtils.shutdownAndAwaitTermination(this.writerExecutorService);
    }


}
