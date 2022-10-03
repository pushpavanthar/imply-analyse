package com.imply.analytics.service;


public class SplitHandler implements ILineHandler<String>{

    private final SplitFileWriterService splitFileWriterService;

    public SplitHandler(SplitFileWriterService splitFileWriterService) {
        this.splitFileWriterService = splitFileWriterService;
    }


    public void handle(String line) {
        try {
            splitFileWriterService.produce(line);
        } catch (InterruptedException ex) {
            System.out.println(ex);
        }
    }
}
