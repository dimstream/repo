package com.vmware.dim.output;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.conn.RedisConnection;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;

import redis.clients.jedis.JedisCommands;

/**
 * Implementation of interface {@link OutputStream}<br>
 * An abstract class whichdefines the process of taking the data from redis
 * and injecting into different output targets
 * 
 * @author ghimanshu
 *
 */
public abstract class OutputStreamWriter implements OutputStream {

	private static final Logger logger = LogManager.getLogger(OutputStreamWriter.class);
	private JedisCommands jedis;
	private ConfigurationDetailsDTO configurationDetailsDTO;
	private String outputStreamName;
	private boolean canRun = true;
	private long interval;
	protected Map<String, String> configuration;

	public OutputStreamWriter(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		this.configurationDetailsDTO = configurationDetailsDTO;
		initConfiguration();
		jedis = RedisConnection.getConnection();
		outputStreamName = configurationDetailsDTO.streamName;
		interval = Long.valueOf(configurationDetailsDTO.configurationFrequency) * 1000;
	}

	/**
	 * Method to stop the different output threads
	 */
	public void stop() {
		canRun = false;
	}

	/**
	 * Method to retrieve data from Redis and remove the retrieved key from Redis
	 */
	@Override
	public void run() {
		logger.traceEntry();
		while (canRun) {
			try {
				DataResponse dataResponse = getData();
				write(dataResponse.data);
				removeData(dataResponse.keyset);
			} catch (Exception e) {
				logger.error("Error writing data to output target", e);
			} finally {
				obeyInterval();
			}
		}
		logger.traceExit();
	}

	private void obeyInterval() {
		try {
			Thread.sleep(interval);
		} catch (InterruptedException e) {
			canRun = false;
			logger.error("Thread Stopped. ", e);
		}
	}

	private DataResponse getData() throws Exception {
		logger.traceEntry(configurationDetailsDTO.configurationName);
		Map<String, String> redisData = new HashMap<String, String>();
		Set<String> globalAliasSet = jedis
				.smembers(configurationDetailsDTO.configurationName + "_Output_" + outputStreamName);

		Iterator<String> globalAliasIterator = globalAliasSet.iterator();
		while (globalAliasIterator.hasNext()) {
			String key = globalAliasIterator.next();
			logger.debug("Key:" + key);
			String hashValue = jedis.hget(key, "json");
			String hashKey;
			if (key.contains("Context")) {
				hashKey = key.substring(outputStreamName.length() + 9);
			} else {
				// Stream
				hashKey = key.substring(outputStreamName.length() + 8);
			}

			if (null != hashValue) {
				redisData.put(hashKey, hashValue);
			}
		}
		DataResponse dataResponse = new DataResponse();
		dataResponse.data = redisData;
		dataResponse.keyset = globalAliasSet;
		return logger.traceExit(dataResponse);
	}

	private void removeData(Set<String> globalAliasSet) {
		for (String st : globalAliasSet) {
			jedis.srem(configurationDetailsDTO.configurationName + "_Output_" + outputStreamName, st);
		}
	}

	protected void initConfiguration() {

		logger.traceEntry();
		configuration = new HashMap<String, String>();

		Iterator<ConfigurationPropertiesDTO> dtoIterator = configurationDetailsDTO.configurationProperties
				.iterator();
		while (dtoIterator.hasNext()) {
			ConfigurationPropertiesDTO pDTO = (ConfigurationPropertiesDTO) dtoIterator.next();
			configuration.put(pDTO.configurationKey, pDTO.configurationValue);
		}
		logger.traceExit();
	}

	private class DataResponse {
		public Map<String, String> data;
		public Set<String> keyset;
	}

}