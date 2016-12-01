package com.vmware.dim.impl;

import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class implementing thread to roll files in HDFS as per configured file roll interval
 * 
 * @author ghimanshu
 *
 */
public class HDFSFileRollover implements Runnable {

	private static final Logger logger = LogManager.getLogger(HDFSOutputStream.class);

	private List<FSDataOutputStream> dataOutputStreams;
	private boolean canRun = true;
	private long fileRolloverInterval;

	public HDFSFileRollover(List<FSDataOutputStream> dataOutputStreams, long fileRolloverInterval) {
		this.dataOutputStreams = dataOutputStreams;
		this.fileRolloverInterval = fileRolloverInterval;
	}

	private void rollover() throws Exception {
		synchronized (dataOutputStreams) {
			if (!dataOutputStreams.isEmpty() && dataOutputStreams.get(0).getPos() > 0) {
				logger.debug("Closing file for rollover ");
				dataOutputStreams.clear();
			}
		}
	}

	private void obeyInterval() {
		try {
			Thread.sleep(fileRolloverInterval);
		} catch (InterruptedException e) {
			canRun = false;
		}
	}

	@Override
	public void run() {
		while (canRun) {
			try {
				obeyInterval();
				rollover();
			} catch (Exception e) {
				logger.error("Error in rollover thread ", e);
			}
		}
	}
}