package com.vmware.dim.impl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;
import com.vmware.dim.input.InputStreamReader;

/**
 * RMQ input source class implementing {@link InputStreamReader}<br>
 * Continuously monitor the Rabbit Messaging queue and pushes to DIM's Kafka DataBus
 * 
 * @author ghimanshu
 *
 */
public class RMQInputStream extends InputStreamReader {

	private Connection connection;
	private Channel channel;
	String rabbitQueName;

	private static final Logger logger = LogManager.getLogger(RMQInputStream.class);

	public RMQInputStream(ConfigurationDetailsDTO confDetailsDTO, KafkaCoordinatesDTO kafkaCoordinates) throws Exception {

		super(confDetailsDTO, kafkaCoordinates);
		logger.traceEntry();

		String rabbitHost = null;
		String rabbitUsername = null;
		String rabbitPassword = null;
		String rabbitVHost = null;
		int rabbitPort = 0;

		Iterator<ConfigurationPropertiesDTO> dtoIterator = confDetailsDTO.configurationProperties.iterator();
		while (dtoIterator.hasNext()) {
			ConfigurationPropertiesDTO pDTO = (ConfigurationPropertiesDTO) dtoIterator.next();
			if ("RabbitHost".equals(pDTO.configurationKey))
				rabbitHost = pDTO.configurationValue;
			else if ("RabbitQueName".equals(pDTO.configurationKey))
				rabbitQueName = pDTO.configurationValue;
			else if ("RabbitUsername".equals(pDTO.configurationKey))
				rabbitUsername = pDTO.configurationValue;
			else if ("RabbitPassword".equals(pDTO.configurationKey))
				rabbitPassword = pDTO.configurationValue;
			else if ("RabbitVHost".equals(pDTO.configurationKey))
				rabbitVHost = pDTO.configurationValue;
			else if ("RabbitPort".equals(pDTO.configurationKey))
				rabbitPort = Integer.parseInt(pDTO.configurationValue);
		}

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(rabbitHost);
		factory.setUsername(rabbitUsername);
		factory.setPassword(rabbitPassword);
		factory.setPort(rabbitPort);
		factory.setVirtualHost(rabbitVHost);

		connection = factory.newConnection();
		channel = connection.createChannel();
		logger.traceExit();
	}

	/**
	 * Method to read data from RMQ
	 */
	@Override
	public void run() {
		logger.traceEntry();
		try {
			boolean autoAck = false;
			channel.basicConsume(rabbitQueName, autoAck, "", new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					try {
						execute(new String(body));
					} catch (Exception e) {
						logger.error("Error handling delivery - ", e);
					} finally {
						channel.basicAck(envelope.getDeliveryTag(), false);
					}
				}
			});
		} catch (Exception e) {
			logger.error("Error consuming messages - ", e);
		}
		logger.traceExit();
	}

	/**
	 * Method to stop the RMQ Input thread
	 */
	@Override
	public void stop() throws Exception {
		channel.abort();
		connection.close();
	}
}