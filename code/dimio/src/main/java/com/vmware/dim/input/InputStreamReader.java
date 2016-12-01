package com.vmware.dim.input;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;
import com.vmware.common.trap.TrapAction;

/**
 * Implementation of interface {@link InputStream}<br>
 * An abstract class which defines the process of inserting the data from
 * different input sources into DIM kafka
 * 
 * @author ghimanshu
 *
 */
public abstract class InputStreamReader implements InputStream {

	private KafkaCoordinatesDTO kafkaCoordinates;
	private Producer<Integer, String> producer;

	private static final Logger logger = LogManager.getLogger(InputStreamReader.class);
	protected String connName;

	public InputStreamReader(ConfigurationDetailsDTO confDetailsDTO, KafkaCoordinatesDTO kafkaCoordinates) {
		logger.traceEntry();
		connName = confDetailsDTO.configurationName;
		this.kafkaCoordinates = kafkaCoordinates;

		Properties props = new Properties();

		props.put("acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 0);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);
		props.put("bootstrap.servers", kafkaCoordinates.metaDataBrokerList);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<Integer, String>(props);
		logger.traceExit();
	}

	/**
	 * Method to insert the data into DIM's Kafka
	 */
	@Override
	public int execute(String... data) {

		logger.traceEntry();
		try {
			for (String payload : data) {
				logger.debug("Data = " + payload);
				if (TrapAction.trapAction("ip:" + connName, payload)) {
					logger.debug("Inserting data into Kafka at Topic=" + kafkaCoordinates.topic);
					ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(kafkaCoordinates.topic,
							payload);
					producer.send(producerRecord);
				}
			}
			return logger.traceExit(0);
		} catch (Exception e) {
			logger.error("Issue with DIM Input execution", e);
			return logger.traceExit(1);
		}
	}
}