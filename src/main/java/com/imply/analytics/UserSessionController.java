package com.imply.analytics;

import com.imply.analytics.service.*;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UserSessionController {
    int threadSize = 10;
    int partitionCount = 10;
    ExecutorService sharedService;

    CountMapperService countMapperService;
    SplitFileWriterService splitFileWriterService;
    SplitHandler splitHandler;
    FileReaderService fileReaderService;
    FilterMapper filterMapper;
    CountDownLatch latch;

    public UserSessionController() {
        latch = new CountDownLatch(1);
        sharedService = Executors.newFixedThreadPool(threadSize);
        countMapperService = new CountMapperService(sharedService, "src/main/resources/splotsO", partitionCount, latch);
        splitFileWriterService = new SplitFileWriterService(sharedService, "src/main/resources/splits",partitionCount, new SimpleHashPartitioner(partitionCount), countMapperService);
        splitHandler = new SplitHandler(splitFileWriterService);
        fileReaderService = new FileReaderService.Builder("src/main/resources/access.log", splitHandler)
            .withTreahdSize(threadSize)
            .withBufferSize(1024*1024)
            .withSharedExecutorService(Executors.newFixedThreadPool(threadSize))
            .withNextService(splitFileWriterService)
            .build();

        filterMapper = new FilterMapper("src/main/resources/splitsO", "src/main/resources/result/output.txt");
    }

    public void initialize() throws InterruptedException {
        System.out.println(System.currentTimeMillis());
        splitFileWriterService.start();
        fileReaderService.start();
        latch.await();
    }

    public void generateFilteredUsers(Integer input) throws FileNotFoundException {
//        try{
//            Thread.sleep(60000);
//        }catch (InterruptedException ignored) {
//
//        }

        filterMapper.start(input);
    }
}
