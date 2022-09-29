package com.imply.analytics;

import com.imply.analytics.service.*;

public class ApplicationMain {

	public static void main(String[] args) {
		FileWriterTask fileWriterTask = new FileWriterTask("src/main/resources/splits",10, new SimpleHashPartitioner(10));
		SplitHandler splitHandler = new SplitHandler(fileWriterTask);
//		FileReaderService.Builder builder = new FileReaderService.Builder("src/main/resources/access.log",new ILineHandler<String>() {
//
//			public void handle(String line) {
////				System.out.println(line);
////				increat();
//			}
//		});
		FileReaderService.Builder builder = new FileReaderService.Builder("src/main/resources/access.log",splitHandler);
		builder.withTreahdSize(10)
			   .withBufferSize(1024*1024);
		FileReaderService fileReader = builder.build();
		fileReader.start();

		fileReader.shutdown();
		fileWriterTask.shutdown();
		//use ShutdownHook to make any clean up logic on exit.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try { fileReader.shutdown(); }
				catch (Exception ignored) {}
			}
		});

	}
	
}
