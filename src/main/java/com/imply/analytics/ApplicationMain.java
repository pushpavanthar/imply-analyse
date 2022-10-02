package com.imply.analytics;

import com.imply.analytics.service.*;
import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.util.concurrent.CyclicBarrier;

public class ApplicationMain {

	public static void main(String[] args) throws FileNotFoundException {
		System.out.println(System.currentTimeMillis());
		CyclicBarrier barrier = new CyclicBarrier(1, new Runnable() {
			@SneakyThrows
			@Override
			public void run() {
				CountMapper countMapper = new CountMapper("src/main/resources/splotsO", 10);
				countMapper.start();
				System.out.println("Starting CountMapper Executor Service");
			}
		});
		FileWriterTask fileWriterTask = new FileWriterTask("src/main/resources/splits",10, new SimpleHashPartitioner(10), barrier);
		SplitHandler splitHandler = new SplitHandler(fileWriterTask);
//		FileReaderService.Builder builder = new FileReaderService.Builder("src/main/resources/access.log",new ILineHandler<String>() {
//
//			public void handle(String line) {
////				System.out.println(line);
////				increat();
//			}
//		});
		FileReaderService.Builder builder = new FileReaderService.Builder("src/main/resources/access.log", splitHandler);
		builder.withTreahdSize(10)
			   .withBufferSize(1024*1024);
		FileReaderService fileReader = builder.build();
		fileReader.start();



		FilterMapper filterMapper = new FilterMapper("src/main/resources/splitsO", "src/main/resources/result/output.txt");
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		filterMapper.start(25);

//		fileReader.shutdown();
//		fileWriterTask.shutdown();
		//use ShutdownHook to make any clean up logic on exit.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try { fileReader.shutdown(); }
				catch (Exception ignored) {}
			}
		});

	}
	
}
