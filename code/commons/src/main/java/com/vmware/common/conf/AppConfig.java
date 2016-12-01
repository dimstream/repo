package com.vmware.common.conf;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.JsonPath;
import com.vmware.common.conn.RedisConnection;

/**
 * Class to parse the input application configuration and maps to a POJO
 * <br>
 * 
 * <b>Sample Configuration</b>
 * <pre>
 * {@code  
 * 
 * {	"derbyUrl": "jdbc:derby:ManagerDB;create=true",
 *    "apiConfiguration": {
 *        "port": 9090,
 *        "maxThreads": 5
 *    },
 *   "managerAgent": {
 *       "port": 9091,
 *       "host": "fmw-dev-build-1"
 *   },
 *   "kafkaConfiguration": {
 *       "metaDataBrokerList": "localhost:9092",
 *      "serializerClass": "kafka.serializer.StringEncoder",
 *      "keySerializerClass": "kafka.serializer.StringEncoder",
 *       "topic":"DataBus1"
 *   }, 
 *   "redisConfiguration": [{
 *		"redisMachine": "localhost",
 *		"redisPort": 6379
 *	}]
 * }
 * 
 * }
 * </pre>
 * 
 * @author vedanthr
 */
public class AppConfig {
	public static int apiPort;
	public static int apiMaxThreads;
	public static String connectionURL;
	public static Integer agentPort;
	public static String agentHost;
	public static String checkpointFile;
	public static String metaDataBrokerList;
	public static String serializerClass;
	public static String keySerializerClass;
	public static String topic;
	public static List<String> redisMachine;
	public static List<Integer> redisPort;

	private static final Logger logger = LogManager.getLogger(AppConfig.class);

	/**
	 * Reads the input configuration json using {@link JsonPath} library
	 * and maps to local variables. 
	 * 
	 * @param configurationFile Input configuration file
	 */
	public static void readApplicationConfiguration(String configurationFile) {

		logger.traceEntry();
		String configurationJson = null;
		logger.debug("Reading Application Configuration from file - " + configurationFile);
		try {
			configurationJson = FileUtils.readFileToString(new File(configurationFile));
			logger.info(configurationJson);
			logger.debug("End of Configuration JSON");

			// Read DB properties as individual strings
			connectionURL = JsonPath.parse(configurationJson).read("$.connectionURL", String.class);
			checkpointFile = JsonPath.parse(configurationJson).read("$.checkpointFile", String.class);
			apiPort = JsonPath.parse(configurationJson).read("$.apiConfiguration.port", Integer.class);
			apiMaxThreads = JsonPath.parse(configurationJson).read("$.apiConfiguration.maxThreads", Integer.class);
			agentPort = JsonPath.parse(configurationJson).read("$.managerAgent.port", Integer.class);
			agentHost = JsonPath.parse(configurationJson).read("$.managerAgent.host", String.class);
			metaDataBrokerList = JsonPath.parse(configurationJson).read("$.kafkaConfiguration.metaDataBrokerList",
					String.class);
			serializerClass = JsonPath.parse(configurationJson).read("$.kafkaConfiguration.serializerClass",
					String.class);
			keySerializerClass = JsonPath.parse(configurationJson).read("$.kafkaConfiguration.keySerializerClass",
					String.class);
			topic = JsonPath.parse(configurationJson).read("$.kafkaConfiguration.topic", String.class);
			redisMachine = JsonPath.parse(configurationJson).read("$.redisConfiguration[*].redisMachine");
			redisPort = JsonPath.parse(configurationJson).read("$.redisConfiguration[*].redisPort");

			RedisConnection.init(redisMachine, redisPort);

		} catch (Exception e) {
			logger.error(
					"Error reading Configuration file. Please check if the file exists, and has the correct configuations",
					e);
		}
		logger.traceExit();
	}
}
