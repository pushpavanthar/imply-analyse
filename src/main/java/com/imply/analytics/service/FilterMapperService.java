package com.imply.analytics.service;

import com.imply.analytics.util.IConstants;
import com.imply.analytics.util.Util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class FilterMapperService {

    private final String srcLocation;
    private final String filteredOutput;

    public FilterMapperService(String inputDir, String filteredOutput) {
        this.srcLocation = inputDir;
        this.filteredOutput = filteredOutput;
    }

    public void start(Integer filter) throws FileNotFoundException {
        initialize();
        File fout = new File(filteredOutput + IConstants.FILE_SEPARATOR + "results.txt");
        FileOutputStream fos = new FileOutputStream(fout);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        AtomicInteger counter = new AtomicInteger(0);
        try (Stream<Path> paths = Files.walk(Paths.get(srcLocation))) {
            StringBuilder sb = new StringBuilder();
            paths
                .filter(Files::isRegularFile)
                .forEach(x -> {
                    try {
                        File file = x.toFile();
                        FileReader fr = new FileReader(file);
                        BufferedReader br = new BufferedReader(fr);
                        String line;

                        while((line = br.readLine())!=null)
                        {
                            String[] rowSplit = line.split("\\|");
                            if(rowSplit.length==2) {
                                int length = rowSplit[1].split(",").length;
                                if(length >= filter) {
                                    counter.incrementAndGet();
                                    sb.append(rowSplit[0]).append("\n");
                                    if(sb.length() > 1024) {
                                        osw.write(sb.toString());
                                        sb.setLength(0);
                                    }

                                }
                            }
                        }
                        osw.write(sb.toString());
                        sb.setLength(0);
                        fr.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            osw.close();
            System.out.println("Total filtered Users visiting " + filter + " distinct pages = "+counter.get());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void initialize() {
        Util.cleanUp(this.filteredOutput);
    }


    public void shutdown(){
    }


}
