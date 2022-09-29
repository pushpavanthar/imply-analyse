package com.imply.analytics.service;


public class SplitHandler implements ILineHandler<String>{

    private final FileWriterTask fileWriterTask;

    public SplitHandler(FileWriterTask fileWriterTask) {
        this.fileWriterTask = fileWriterTask;
    }


    public void handle(String line) {
        try {
            fileWriterTask.produce(line);
        } catch (InterruptedException ex) {
            System.out.println(ex);
        }
    }
}
