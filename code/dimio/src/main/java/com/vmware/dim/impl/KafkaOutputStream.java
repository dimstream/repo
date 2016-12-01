package com.vmware.dim.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * Kafka output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to Kafka
 * 
 * @author ghimanshu
 *
 */
public class KafkaOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(KafkaOutputStream.class);
	private Producer<Integer, String> producer;
	private String topic;

	public KafkaOutputStream(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		super(configurationDetailsDTO);
		logger.traceEntry();

		Properties props = new Properties();
		props.put("acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 0);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);
		props.put("bootstrap.servers", configuration.get("MetaDataBrokerList"));
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		topic = configuration.get("Topic");

		producer = new KafkaProducer<Integer, String>(props);
		logger.traceExit();
	}

	/**
	 * Method to write data to JDBC for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {
		logger.traceEntry();
		try {
			for (String name : redisData.keySet()) {
				logger.debug("name = " + name);
				String value = redisData.get(name);
				logger.debug("value = " + value);
				ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, value);
				producer.send(producerRecord);
			}
		} catch (Exception exception) {
			logger.error("Error inserting data to Kafka", exception);

			logger.traceExit();
		}
	}
}