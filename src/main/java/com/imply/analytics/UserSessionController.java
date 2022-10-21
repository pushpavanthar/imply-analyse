package com.imply.analytics;

import com.imply.analytics.model.SimpleHashPartitioner;
import com.imply.analytics.service.*;
import com.imply.analytics.util.IConstants;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;
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
    FilterMapperService filterMapper;
    CountDownLatch latch;

    public UserSessionController(String inputFile) {
        latch = new CountDownLatch(1);
        sharedService = Executors.newFixedThreadPool(threadSize);
        countMapperService = new CountMapperService(sharedService, IConstants.MAP0_LOCATION, IConstants.MAP1_LOCATION, partitionCount, latch);
        splitFileWriterService = new SplitFileWriterService(sharedService, IConstants.MAP0_LOCATION,partitionCount, new SimpleHashPartitioner(partitionCount), countMapperService);
        splitHandler = new SplitHandler(splitFileWriterService);
        fileReaderService = new FileReaderService.Builder(inputFile, splitHandler)
            .withTreahdSize(threadSize)
            .withBufferSize(1024*1024)
            .withSharedExecutorService(Executors.newFixedThreadPool(threadSize))
            .withNextService(splitFileWriterService)
            .build();

        filterMapper = new FilterMapperService(IConstants.MAP1_LOCATION, IConstants.OUTPUT_FILE_LOCATION );
    }

    public void initialize() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        System.out.println(startTime);
        splitFileWriterService.start();
        fileReaderService.start();
        latch.await();
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to complete reading input file and generating userId maps = "+ (endTime - startTime));
    }

    public void generateFilteredUsers(Integer input) throws FileNotFoundException {
        filterMapper.start(input);
    }
}
