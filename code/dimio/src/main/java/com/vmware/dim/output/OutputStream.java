package com.vmware.dim.output;

import java.util.Map;

/**
 * Output Parent Interface representing the two step process of data transfer from DIM Framework
 * 
 * @author ghimanshu
 *
 */
public interface OutputStream extends Runnable {
	public void write(Map<String, String> redisData);
}
