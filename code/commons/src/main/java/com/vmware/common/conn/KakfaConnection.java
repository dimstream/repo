package com.vmware.common.conn;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import redis.clients.jedis.JedisCommands;

/**
 * Class to establish connection with the Redis cluster.
 * 
 * @author vedanthr
 */
public class KakfaConnection {

	static JedisCommands jedis = null;

	private static final Logger logger = LogManager.getLogger(KakfaConnection.class);
	private static Producer<Integer, String> producer;
	private static String topic;

	public static void init() throws SQLException {
		logger.traceEntry();

		JedisCommands jedis = RedisConnection.getConnection();

		Properties props = new Properties();
		props.put("acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 0);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);
		props.put("bootstrap.servers", jedis.hget("Kafka", "MetaDataBrokerList"));
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		topic = jedis.hget("Kafka", "Topic");

		producer = new KafkaProducer<Integer, String>(props);
		logger.traceExit();
	}

	public static void writeToKafka(String payload) {
		logger.traceEntry(payload);
		try {
			if (producer == null) {
				init();
			}
			ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, payload);
			producer.send(producerRecord);
		} catch (Exception exception) {
			logger.error("Error inserting data to Kafka", exception);
		}
		logger.traceExit();
	}
}