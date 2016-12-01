package com.vmware.dim.impl;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;
import com.vmware.dim.input.InputStreamReader;

/**
 * Kafka input source class implementing {@link InputStreamReader}<br>
 * Polls the data from Kafka after every configured interval and pushes to DIM's Kafka DataBus
 * 
 * @author ghimanshu
 *
 */
public class KafkaInputStream extends InputStreamReader {

	private KafkaConsumer<String, String> consumer;
	private String topic;
	private static final Logger logger = LogManager.getLogger(KafkaInputStream.class);
	private boolean canRun = true;

	public KafkaInputStream(ConfigurationDetailsDTO confDetailsDTO, KafkaCoordinatesDTO kafkaCoordinates)
			throws Exception {

		super(confDetailsDTO, kafkaCoordinates);
		logger.traceEntry();

		String metaDataBrokerList = null;
		String keyDeserializer = null;
		String valueDeserializer = null;
		String groupID = null;

		Iterator<ConfigurationPropertiesDTO> dtoIterator = confDetailsDTO.configurationProperties.iterator();
		while (dtoIterator.hasNext()) {
			ConfigurationPropertiesDTO pDTO = (ConfigurationPropertiesDTO) dtoIterator.next();
			if ("MetaDataBrokerList".equals(pDTO.configurationKey))
				metaDataBrokerList = pDTO.configurationValue;
			else if ("KeyDeserializer".equals(pDTO.configurationKey))
				keyDeserializer = pDTO.configurationValue;
			else if ("ValueDeserializer".equals(pDTO.configurationKey))
				valueDeserializer = pDTO.configurationValue;
			else if ("Topic".equals(pDTO.configurationKey))
				topic = pDTO.configurationValue;
			else if ("GroupID".equals(pDTO.configurationKey))
				groupID = pDTO.configurationValue;
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", metaDataBrokerList);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("group.id", groupID);
		props.put("key.deserializer", keyDeserializer);
		props.put("value.deserializer", valueDeserializer);

		consumer = new KafkaConsumer<String, String>(props);
		logger.traceExit();
	}

	/**
	 * Method to read data from Kafka (input source)
	 */
	@Override
	public void run() {
		logger.traceEntry();
		try {
			consumer.subscribe(Arrays.asList(topic));
			while (canRun) {
				ConsumerRecords<String, String> records = consumer.poll(5000);
				logger.debug("## Records count = " + records.count());
				for (ConsumerRecord<String, String> record : records) {
					logger.info(record.value());
					execute(record.value());
				}
			}
		} catch (Exception e) {
			logger.error("Error consuming messages from Kafka- ", e);
		}
		logger.traceExit();
	}

	/**
	 * Method to stop the Kafka Input thread
	 */
	@Override
	public void stop() throws Exception {
		canRun = false;
	}
}