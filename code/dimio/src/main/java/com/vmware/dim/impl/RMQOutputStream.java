package com.vmware.dim.impl;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * RMQ output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to RMQ
 * 
 * @author ghimanshu
 *
 */
public class RMQOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(RMQOutputStream.class);

	private String rabbitExchangeName = null;
	private String rabbitRoutingKey = null;
	private Connection connection = null;
	private Channel channel = null;

	public RMQOutputStream(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		super(configurationDetailsDTO);
		logger.traceEntry();
		rabbitExchangeName = configuration.get("RabbitExchangeName");
		rabbitRoutingKey = configuration.get("RabbitRoutingKey");

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(configuration.get("RabbitHost"));
		factory.setUsername(configuration.get("RabbitUsername"));
		factory.setPassword(configuration.get("RabbitPassword"));
		factory.setPort(Integer.parseInt(configuration.get("RabbitPort")));
		factory.setVirtualHost(configuration.get("RabbitVHost"));

		connection = factory.newConnection();
		channel = connection.createChannel();
		logger.traceExit();
	}

	/**
	 * Method to write data to RMQ for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {
		logger.traceEntry();
		for (String name : redisData.keySet()) {
			String value = redisData.get(name);
			try {
				channel.basicPublish(rabbitExchangeName, rabbitRoutingKey, null, value.getBytes());
			} catch (Exception e) {
				logger.error("Error publishing data to RMQ ", e);
			}
		}
		logger.traceExit();
	}
}