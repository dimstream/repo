package com.vmware.process;

import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import com.vmware.common.checkpoint.Checkpoint;
import com.vmware.common.conf.AppConfig;
import com.vmware.conn.DBConnection;
import com.vmware.manage.Manager;

/**
 * Starting point of the Manager API<br>
 * It reads the configuration file and starts listening on the configured TCP Port for incoming HTTP Requests<br>
 * 
 * <b>Sample Configuration: </b>
 * <pre>
 * {@code
 *  {	"derbyUrl": "jdbc:derby:ManagerDB;create=true",
 *       "apiConfiguration": {
 *       "port": 9090,
 *       "maxThreads": 5
 *   },
 *   "managerAgent": {
 *       "port": 9091,
 *       "host": "fmw-dev-build-1"
 *   },
 *   "kafkaConfiguration": {
 *       "metaDataBrokerList": "localhost:9092",
 *       "serializerClass": "kafka.serializer.StringEncoder",
 *       "keySerializerClass": "kafka.serializer.StringEncoder",
 *       "topic":"DataBus1"
 *   }, 
 *	"redisConfiguration": [{
 *		"redisMachine": "localhost",
 *		"redisPort": 6379
 *	}]
 * }
 *}
 * </pre>
 * 
 * @author ghimanshu
 *
 */
public class ManagerAPI {

	private static final Logger logger = LogManager.getLogger(ManagerAPI.class);
	public static String BASE_URI = "http://0.0.0.0:";

	/**
	 * Main method and the starting point of the application
	 * 
	 * @param args Configuration File
	 * @throws Exception Exception initializing the Manager App
	 */
	public static void main(String[] args) throws Exception {
		AppConfig.readApplicationConfiguration(args[0]);
		DBConnection.initializeDB();
		Checkpoint.init();
		
		BASE_URI = BASE_URI + AppConfig.apiPort + "/";
		startServer();
		logger.info("API Active => " + BASE_URI);
		
		Manager.startAllSources();
	}

	public static HttpServer startServer() {
		final ResourceConfig rc = new ResourceConfig().packages("com.vmware.point");
		return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
	}
}
