package com.alibaba.middleware.race.externalsort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.alibaba.middleware.race.externalsort.ExternalSortIndex.TimeKey;

public class ExternalSort {
	private List<String> orderFiles;
	private List<String> storeFolders;
	
	private int sizeoffile = (int)1e11;
	private int maxTmpFiles = 100;
	private int outputFileSize = (int)5e3;
	
	private boolean distinct = false;
	private boolean usegzip = true;
	private File tmpDirectory;	
	private File outputDirectory;
	private String outputFilePrefix = "/sort_order_record";
	
	private ExternalSortIndex externalSortIndex = new ExternalSortIndex();
	
	public ExternalSort(List<String> orderFiles, List<String> storeFolders){
		this.orderFiles = orderFiles;
		this.storeFolders = storeFolders;
		this.tmpDirectory = new File(storeFolders.get(0) + "/tmpDirectory");
		this.outputDirectory = new File(storeFolders.get(0) + "/outputDirectory");
		
		
		if(tmpDirectory.exists()){
			deleteDir(tmpDirectory);
		}
		tmpDirectory.mkdir();
		
		if(outputDirectory.exists()){
			deleteDir(outputDirectory);
		}
		outputDirectory.mkdir();
	}
	
	public void sort(){
		try {
			List<File> tmpFiles = sortInBatch();
			mergeSortedFiles(tmpFiles, outputDirectory, new CreatetimeComparator(), Charset.defaultCharset(), distinct, false, usegzip);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private class CreatetimeComparator implements Comparator<String>{
		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			String[] keyValues = o1.split("\t");
			String createTime1 = null;
			for(String keyValue : keyValues){
				if(keyValue.startsWith("createtime")){
					createTime1 = keyValue.split(":")[1];
					break;
				}
			}
			
			keyValues = o2.split("\t");
			String createTime2 = null;
			for(String keyValue : keyValues){
				if(keyValue.startsWith("createtime")){
					createTime2 = keyValue.split(":")[1];
				}
			}
			return createTime1.compareTo(createTime2);
		}
		
	}
	
	private List<File> sortInBatch() throws IOException{
		List<File> ret = new ArrayList<File>();
		for(String orderFile : orderFiles){
			BufferedReader fbr = new BufferedReader(new InputStreamReader(new FileInputStream(orderFile), Charset.defaultCharset()));
			ret.addAll(sortInBatch(fbr, new CreatetimeComparator(), maxTmpFiles, 
					   estimateAvailableMemory(), Charset.defaultCharset(), 
					   tmpDirectory, distinct, usegzip));
		}
		
		return ret;
	}
	
	private List<File> sortInBatch(final BufferedReader fbr, final Comparator<String> cmp,
                                   final int maxtmpfiles, long maxMemory, final Charset cs,
                                   final File tmpdirectory, final boolean distinct,final boolean usegzip) throws IOException {
            List<File> files = new ArrayList<File>();
            long blocksize = estimateBestSizeOfBlocks(maxtmpfiles, estimateAvailableMemory()); 

            try {
				List<String> tmplist = new ArrayList<String>();
				String line = "";
				try {
				     while (line != null) {
				     long currentblocksize = 0;
				         while ((currentblocksize < blocksize) && ((line = fbr.readLine()) != null)) {
				             tmplist.add(line);
				             currentblocksize += StringSizeEstimator.estimatedSizeOf(line);
				         }
				     files.add(sortAndSave(tmplist, cmp, cs, tmpdirectory, distinct, usegzip));
				     tmplist.clear();
				    }
				} catch (EOFException oef) {
					if (tmplist.size() > 0) {
						files.add(sortAndSave(tmplist, cmp, cs, tmpdirectory, distinct, usegzip));
						tmplist.clear();
					}
				}
            } finally {
                    fbr.close();
            }
            return files;
    }
	
	private long estimateBestSizeOfBlocks(final int maxtmpfiles, final long maxMemory) {
            // we don't want to open up much more than maxtmpfiles temporary
            // files, better run
            // out of memory first.
            long blocksize = sizeoffile / maxtmpfiles + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);

            // on the other hand, we don't want to create many temporary
            // files
            // for naught. If blocksize is smaller than half the free
            // memory, grow it.
            if (blocksize < maxMemory / 2) {
                    blocksize = maxMemory / 2;
            }
            return blocksize;
    }
	
	private File sortAndSave(List<String> tmplist, Comparator<String> cmp, Charset cs, File tmpdirectory,
            				 boolean distinct, boolean usegzip) throws IOException {
            Collections.sort(tmplist, cmp);
            File newtmpfile = File.createTempFile("sortInBatch", "flatfile", tmpdirectory);
            newtmpfile.deleteOnExit();
           
            OutputStream out = new FileOutputStream(newtmpfile);
            int ZIPBUFFERSIZE = 2048;
            
            if (usegzip){
	            out = new GZIPOutputStream(out, ZIPBUFFERSIZE) {
	                    {
	                            this.def.setLevel(Deflater.BEST_SPEED);
	                    }
	            };
            }
            
            BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(out, cs));
            try {
				if (!distinct) {
				    for (String r : tmplist) {
				                fbw.write(r);
				                fbw.newLine();
				    }
				} else {
					String lastLine = null;
					Iterator<String> i = tmplist.iterator();
					if(i.hasNext()) {
						lastLine = i.next();
						fbw.write(lastLine);
						fbw.newLine();
					}
					while (i.hasNext()) {
						String r = i.next();
						// Skip duplicate lines
						if (cmp.compare(r, lastLine) != 0) {
							fbw.write(r);
							fbw.newLine();
							lastLine = r;
						}
					}
				}
            } finally {
                fbw.close();
            }
            return newtmpfile;
    }
	
	public int mergeSortedFiles(List<File> files, File outputDirectory,
            					final Comparator<String> cmp, Charset cs, boolean distinct,
            					boolean append, boolean usegzip) throws IOException {
            ArrayList<BinaryFileBuffer> bfbs = new ArrayList<BinaryFileBuffer>();
            for (File f : files) {
                final int BUFFERSIZE = 2048;
                InputStream in = new FileInputStream(f);
                BufferedReader br;
                
                if(usegzip){
                    br = new BufferedReader(new InputStreamReader(new GZIPInputStream(in, BUFFERSIZE), cs));
                }else{
                	br = new BufferedReader(new InputStreamReader(in, cs));
                }

                BinaryFileBuffer bfb = new BinaryFileBuffer(br);
                bfbs.add(bfb);
            }
            
            int rowcounter = mergeSortedFiles(outputDirectory, cmp, distinct, bfbs);
            
            for (File f : files){
                f.delete();
            }
            return rowcounter;
    }
	
	public int mergeSortedFiles(File outputDirectory, final Comparator<String> cmp, boolean distinct,	
            					List<BinaryFileBuffer> buffers) throws IOException {
		
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(11, new Comparator<BinaryFileBuffer>() {
            @Override
            public int compare(BinaryFileBuffer i,
                    BinaryFileBuffer j) {
                    return cmp.compare(i.peek(), j.peek());
            }
        });
            
        for (BinaryFileBuffer bfb : buffers){
            if (!bfb.empty()){
            	pq.add(bfb);
            }
        }
        
        int outputFileIndex = 0;
		int currentFileSize = 0;
		long currentFileBeginTime = 0;
		long currentFileEndTime = 0;
		boolean firstLine = true;
		
		File outputFile = new File(outputDirectory + outputFilePrefix + outputFileIndex +".txt");
		BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, true), Charset.defaultCharset()));    
		        
        int rowcounter = 0;
        try {                        
            if(!distinct) {
                while (pq.size() > 0) {
                        BinaryFileBuffer bfb = pq.poll();
                        String r = bfb.pop();
                        fbw.write(r);
                        fbw.newLine();
                        if(firstLine){
                        	String[] items = r.split("\t");
                        	for(String item : items){
                        		String[] keyValue = item.split(":");
                        		if(keyValue[0].equals("createtime")){
                        			currentFileBeginTime = Long.valueOf(keyValue[1]);
                        			firstLine = false;
                        			break;
                        		}
                        	}
                        }
                        
                        currentFileSize += StringSizeEstimator.estimatedSizeOf(r);
                        if(currentFileSize > outputFileSize){
                        	String[] items = r.split("\t");
                        	for(String item : items){
                        		String[] keyValue = item.split(":");
                        		if(keyValue[0].equals("createtime")){
                        			currentFileEndTime = Long.valueOf(keyValue[1]);
                        			break;
                        		}
                        	}
                        	externalSortIndex.addIndex(new TimeKey(currentFileBeginTime, currentFileEndTime), outputFileIndex);
                        	
                        	firstLine = true;                        	
                        	fbw.close();
                        	outputFileIndex++;
                        	currentFileSize = 0;
                        	
                        	outputFile = new File(outputDirectory + outputFilePrefix + outputFileIndex +".txt");
                        	fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, true), Charset.defaultCharset()));    
                        }
                        
                        ++rowcounter;
                        if (bfb.empty()) {
                            bfb.fbr.close();
                        }else {
                            pq.add(bfb);
                        }
                }
            } else {    
                String lastLine = null;
                if(pq.size() > 0) {
         			BinaryFileBuffer bfb = pq.poll();
         			lastLine = bfb.pop();
         			fbw.write(lastLine);
         			fbw.newLine();
         			++rowcounter;
         			if (bfb.empty()) {
         				bfb.fbr.close();
         			} else {
         				pq.add(bfb); // add it back
         			}                			
         		}
                while (pq.size() > 0) {
        			BinaryFileBuffer bfb = pq.poll();
        			String r = bfb.pop();
        			// Skip duplicate lines
        			if  (cmp.compare(r, lastLine) != 0) {
        				fbw.write(r);
        				fbw.newLine();
        				lastLine = r;
        			}
        			++rowcounter;
        			if (bfb.empty()) {
        				bfb.fbr.close();
        			} else {
        				pq.add(bfb); // add it back
        			}
                }
            }
       } finally {
            fbw.close();
            for(BinaryFileBuffer bfb : pq)
                bfb.close();
       }
       return rowcounter;

    }
	
	final class BinaryFileBuffer {
        public BinaryFileBuffer(BufferedReader r) throws IOException {
                this.fbr = r;
                reload();
        }
        public void close() throws IOException {
                this.fbr.close();
        }

        public boolean empty() {
                return this.cache == null;
        }

        public String peek() {
                return this.cache;
        }

        public String pop() throws IOException {
                String answer = peek().toString();// make a copy
                reload();
                return answer;
        }

        private void reload() throws IOException {
                this.cache = this.fbr.readLine();
        }

        public BufferedReader fbr;

        private String cache;
	}
	
	public long estimateAvailableMemory() {
        System.gc();
        return Runtime.getRuntime().freeMemory();
	}
	
	private static void deleteDir(File f) {      
		if(f.exists() && f.isDirectory()){
		   if(f.listFiles().length==0){
		       f.delete();  
		    }else{
		       File[] delFile = f.listFiles();  
		       int i = delFile.length;  
		       for(int j = 0; j < i; j++){  
		           if(delFile[j].isDirectory()){  
		        	   deleteDir(delFile[j]);
		           }  
		           delFile[j].delete();
		       } 
	        }  
		}
	}
	
	public static void main(String[] args){
		List<String> orderFiles = new LinkedList<String>();
		orderFiles.add("order_records.txt");
		
		List<String> storeFiles = new LinkedList<String>();
		storeFiles.add(".");
		
		ExternalSort externalSort = new ExternalSort(orderFiles, storeFiles);
		externalSort.sort();
		
		externalSort.externalSortIndex.print();
	}
}
