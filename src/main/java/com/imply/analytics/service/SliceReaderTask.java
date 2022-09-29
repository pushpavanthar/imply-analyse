package com.imply.analytics.service;

import com.imply.analytics.model.StartEndPair;

import java.io.ByteArrayOutputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;



public class SliceReaderTask implements Runnable {
    private final long start;
    private final long sliceSize;
    private final byte[] readBuff;
    private final FileReaderService fileReaderService;


    public SliceReaderTask(StartEndPair pair, FileReaderService fileReaderService) {
        this.start = pair.start;
        this.sliceSize = pair.end - pair.start + 1;
        this.readBuff = new byte[fileReaderService.getBufferSize()];
        this.fileReaderService = fileReaderService;
    }

    @Override
    public void run() {
        try {
            MappedByteBuffer mapBuffer = fileReaderService.getRAccessFile().getChannel().map(FileChannel.MapMode.READ_ONLY, start, this.sliceSize);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int offset = 0; offset < sliceSize; offset += fileReaderService.getBufferSize()) {
                int readLength;
                if (offset + fileReaderService.getBufferSize() <= sliceSize) {
                    readLength = fileReaderService.getBufferSize();
                } else {
                    readLength = (int) (sliceSize - offset);
                }
                mapBuffer.get(readBuff, 0, readLength);
                for (int i = 0; i < readLength; i++) {
                    byte tmp = readBuff[i];
                    if (tmp == '\n' || tmp == '\r') {
                        fileReaderService.handle(bos.toByteArray());
                        bos.reset();
                    } else {
                        bos.write(tmp);
                    }
                }
            }
            if (bos.size() > 0) {
                fileReaderService.handle(bos.toByteArray());
            }
            fileReaderService.getCyclicBarrier().await();//for testing performance
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
