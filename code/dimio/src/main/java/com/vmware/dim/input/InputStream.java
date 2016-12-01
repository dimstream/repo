package com.vmware.dim.input;

/**
 * Input Parent Interface representing the two step process of data ingestion in DIM Framework
 * 
 * @author ghimanshu
 *
 */
public interface InputStream extends Runnable {

	public int execute(String... data);
	public void stop() throws Exception;
}