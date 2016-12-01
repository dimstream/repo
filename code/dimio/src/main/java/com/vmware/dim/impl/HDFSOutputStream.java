package com.vmware.dim.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * HDFS output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to HDFS
 * 
 * @author ghimanshu
 *
 */
public class HDFSOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(HDFSOutputStream.class);

	private FileSystem hadoopFileSystem;
	private Path rootDirectory;
	private List<FSDataOutputStream> dataOutputStreams;

	public HDFSOutputStream(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		super(configurationDetailsDTO);
		logger.traceEntry();

		Configuration conf = new Configuration();
		conf.set("fs.default.name", configuration.get("HDFSLocation"));

		hadoopFileSystem = FileSystem.get(conf);
		dataOutputStreams = new ArrayList<FSDataOutputStream>();

		createRootDirectory(configuration.get("FilePath"));

		Long fileRollInterval = Long.valueOf(configuration.get("FileSplitFrequency")) * 1000;
		Runnable hdfsFileRollover = new HDFSFileRollover(dataOutputStreams, fileRollInterval);
		Thread hdfsFileRolloverThread = new Thread(hdfsFileRollover);
		hdfsFileRolloverThread.start();

		logger.traceExit();
	}

	/**
	 * Creates the HDFS directory, if it doesn't exists
	 * 
	 * @param rootDirectoryPath HDFS Path
	 * @throws Exception
	 */
	private void createRootDirectory(String rootDirectoryPath) throws Exception {
		logger.traceEntry();

		rootDirectory = new Path(rootDirectoryPath);
		if (!hadoopFileSystem.exists(rootDirectory)) {
			logger.warn("Directory " + rootDirectoryPath + " does not exist, creating");
			hadoopFileSystem.mkdirs(rootDirectory);
		}
		logger.traceExit();
	}

	/**
	 * Create new file as per the rollover interval defined
	 * 
	 * @return
	 * @throws Exception
	 */
	private FSDataOutputStream createNewFile() throws Exception {
		logger.traceEntry();
		Path newFile = new Path(rootDirectory, "dim_" + System.currentTimeMillis() + ".json");
		logger.debug("Creating file " + newFile.getName());
		FSDataOutputStream dataOutputStream = null;

		if (!hadoopFileSystem.exists(newFile)) {
			dataOutputStream = hadoopFileSystem.create(newFile);
		} else {
			dataOutputStream = hadoopFileSystem.append(newFile);
		}

		dataOutputStreams.clear();
		dataOutputStreams.add(dataOutputStream);
		logger.traceExit();
		return dataOutputStream;
	}

	private void write(String data) throws Exception {
		if (dataOutputStreams.isEmpty()) {
			createNewFile();
		}
		logger.debug("HDFS File Object " + dataOutputStreams.get(0));
		logger.debug("data " + data);
		dataOutputStreams.get(0).write(data.getBytes());
		dataOutputStreams.get(0).hflush();
	}

	/**
	 * Method to write data to HDFS for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {
		logger.traceEntry();

		synchronized (dataOutputStreams) {
			for (String name : redisData.keySet()) {
				try {
					write(redisData.get(name));
				} catch (Exception e) {
					logger.error("Error writing data in HDFS", e);
				}
			}
		}
		logger.traceExit();

	}
}