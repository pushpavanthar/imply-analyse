package com.imply.analytics.service;

import com.imply.analytics.util.IConstants;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class FrequencyCounterMapperTask implements Runnable{
    private final String dstLocation;
    private final String srcLocation;
    private final Map<Integer, Set<String>> freqCounter;
    private final Integer fileIndex;
    private final CyclicBarrier barrier;

    public FrequencyCounterMapperTask(String srcLocation, String dstLocation, Integer fileIndex, CyclicBarrier barrier) {
        this.fileIndex = fileIndex;
        this.barrier = barrier;
        this.srcLocation = srcLocation;
        this.dstLocation = dstLocation;
        this.freqCounter = new HashMap<>();
    }

    @Override
    public void run() {
        try
        {
            File file = new File( srcLocation + IConstants.FILE_SEPARATOR + fileIndex + IConstants.FILE_SUFFIX);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine())!=null)
            {
                String[] split = line.split(",");
                if(split.length==3) {
                    freqCounter.computeIfAbsent(Integer.valueOf(split[1]), k -> new HashSet<String>());
                    freqCounter.computeIfPresent(Integer.valueOf(split[1]), (k,v) -> {
                        v.add(split[2]);
                        return v;
                    });
                }
            }
            fr.close();
            File fout = new File(dstLocation + IConstants.FILE_SEPARATOR + fileIndex + IConstants.FILE_SUFFIX);
            FileOutputStream fos = new FileOutputStream(fout);

            OutputStreamWriter osw = new OutputStreamWriter(fos);
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Integer, Set<String>> entry : freqCounter.entrySet()) {
                sb.append(entry.getKey()).append("|");
                for(String value : entry.getValue()) {
                    sb.append(value).append(",");
                }
                if(sb.length() > 1) {
                    sb.deleteCharAt(sb.length()-1);
                }
                sb.append("\n");
                osw.write(sb.toString());
                sb.setLength(0);
            }
            osw.close();
            barrier.await();
        } catch (IOException|BrokenBarrierException|InterruptedException e) {
            e.printStackTrace();
        }
    }
}
