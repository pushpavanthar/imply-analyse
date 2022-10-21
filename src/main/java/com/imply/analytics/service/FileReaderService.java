package com.imply.analytics.service;

import com.imply.analytics.model.StartEndPair;
import lombok.Data;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class FileReaderService implements IService{
	private int threadSize;
	private String charset;
	private int bufferSize;
	private ILineHandler<String> handle;
	private ExecutorService sharedExecutorService;
	private long fileLength;
	private RandomAccessFile rAccessFile;
	private Set<StartEndPair> startEndPairs;
	private CyclicBarrier cyclicBarrier;
	private AtomicLong counter = new AtomicLong(0);
	private IService nextService;
	
	private FileReaderService(ExecutorService sharedExecutorService, File file, ILineHandler<String> handle, String charset, int bufferSize, int threadSize, IService nextService){
		this.fileLength = file.length();
		this.handle = handle;
		this.charset = charset;
		this.bufferSize = bufferSize;
		this.threadSize = threadSize;
		try {
			this.rAccessFile = new RandomAccessFile(file,"r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.sharedExecutorService = sharedExecutorService;
		startEndPairs = new HashSet<StartEndPair>();
		this.nextService = nextService;
	}

	@Override
	public void initialize() {
		long everySize = this.fileLength/this.threadSize;
		try {
			calculateStartEnd(0, everySize);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		final long startTime = System.currentTimeMillis();
		cyclicBarrier = new CyclicBarrier(startEndPairs.size(),new Runnable() {

			@Override
			public void run() {
				System.out.println("use time: "+(System.currentTimeMillis()-startTime));
				System.out.println("all line: "+counter.get());
				sharedExecutorService.shutdown();
			}
		});
	}

	public void start(){
		initialize();
		for(StartEndPair pair:startEndPairs){
			System.out.println("Allocate shards ："+ pair);
			this.sharedExecutorService.execute(new SliceReaderTask(pair, this));
		}
	}

	private void calculateStartEnd(long start,long size) throws IOException{
		if(start>fileLength-1){
			return;
		}
		StartEndPair pair = new StartEndPair();
		pair.start=start;
		long endPosition = start+size-1;
		if(endPosition>=fileLength-1){
			pair.end=fileLength-1;
			startEndPairs.add(pair);
			return;
		}
		
		rAccessFile.seek(endPosition);
		byte tmp =(byte) rAccessFile.read();
		while(tmp!='\n' && tmp!='\r'){
			endPosition++;
			if(endPosition>=fileLength-1){
				endPosition=fileLength-1;
				break;
			}
			rAccessFile.seek(endPosition);
			tmp =(byte) rAccessFile.read();
		}
		pair.end=endPosition;
		startEndPairs.add(pair);
		
		calculateStartEnd(endPosition+1, size);
		
	}
	
	
	
	public void teardown(){
		try {
			this.rAccessFile.close();
			this.sharedExecutorService.shutdown();
			this.sharedExecutorService.awaitTermination(60, TimeUnit.SECONDS);

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

	}



	public void handle(byte[] bytes) throws UnsupportedEncodingException{
		String line = null;
		if(this.charset==null){
			line = new String(bytes);
		}else{
			line = new String(bytes,charset);
		}
		if(line!=null && !"".equals(line)){
			this.handle.handle(line);
			counter.incrementAndGet();
		}
	}


	public static class Builder{
		private int threadSize=1;
		private String charset=null;
		private int bufferSize=1024*1024;
		private ILineHandler handle;
		private File file;
		private ExecutorService sharedExecutorService;
		private IService nextService;

		public Builder(String file,ILineHandler handle){
			this.file = new File(file);
			if(!this.file.exists())
				throw new IllegalArgumentException("File does not exist！");
			this.handle = handle;
		}
		
		public Builder withTreahdSize(int size){
			this.threadSize = size;
			return this;
		}
		
		public Builder withCharset(String charset){
			this.charset= charset;
			return this;
		}
		
		public Builder withBufferSize(int bufferSize){
			this.bufferSize = bufferSize;
			return this;
		}

		public Builder withSharedExecutorService(ExecutorService executorService) {
			this.sharedExecutorService = executorService;
			return this;
		}

		public Builder withNextService(IService nextService) {
			this.nextService = nextService;
			return this;
		}
		
		public FileReaderService build(){
			return new FileReaderService(this.sharedExecutorService, this.file,this.handle,this.charset,this.bufferSize,this.threadSize, this.nextService);
		}
	}
	
	
}
