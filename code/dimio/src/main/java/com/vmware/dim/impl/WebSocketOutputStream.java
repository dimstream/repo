package com.vmware.dim.impl;

import java.io.DataOutputStream;
import java.net.Socket;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * Web Socket output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to a configured socket host and port
 * 
 * @author ghimanshu
 *
 */
public class WebSocketOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(WebSocketOutputStream.class);

	public WebSocketOutputStream(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		super(configurationDetailsDTO);
		logger.traceEntry();

		logger.traceExit();
	}

	/**
	 * Method to write data to Web Socket for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {
		logger.traceEntry();
		try(Socket socket1 = new Socket(configuration.get("host"), Integer.parseInt(configuration.get("port")));
				DataOutputStream dout = new DataOutputStream(socket1.getOutputStream())) {
			
			for (String name : redisData.keySet()) {
				logger.debug("name = " + name);
				String value = redisData.get(name);
				logger.debug("value = " + value);
				dout.writeBytes(value);
				dout.flush();
			}
		} catch (Exception exception) {
			logger.error("Error inserting data to Kafka", exception);
			logger.traceExit();
		}
	}
}