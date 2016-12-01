package com.vmware.manage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.vmware.common.checkpoint.Checkpoint;
import com.vmware.common.conf.AppConfig;
import com.vmware.common.conn.RedisConnection;
import com.vmware.common.constants.Constants;
import com.vmware.common.dao.ConfigurationDetails;
import com.vmware.common.dao.EventStream;
import com.vmware.common.dao.ProcessContext;
import com.vmware.common.dao.Trap;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ContextMappingDTO;
import com.vmware.common.dto.ContextMappingDefinitionDTO;
import com.vmware.common.dto.EventStreamDTO;
import com.vmware.common.dto.GlobalConfigurationDTO;
import com.vmware.common.dto.ProcessContextDTO;
import com.vmware.common.dto.TrapDTO;
import com.vmware.common.trap.ObjectConverter;
import com.vmware.dim.agent.InputAgent;
import com.vmware.dim.agent.OutputAgent;

import redis.clients.jedis.JedisCommands;

/**
 * Class to Manage all the operations specified by Manager API
 * 
 * @author ghimanshu
 *
 */
public class Manager {

	private static final Logger logger = LogManager.getLogger(Manager.class);

	/**
	 * Method to Upsert Event Stream
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Event DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static EventStreamDTO upsertStream(String jsonRequest) throws SQLException {

		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		EventStreamDTO streamObj = gson.fromJson(jsonRequest, EventStreamDTO.class);
		ObjectConverter transformer = new ObjectConverter();
		EventStream newObj = transformer.convert(streamObj);
		ManagerDAL layer = new ManagerDAL();

		if (layer.readEventStream(newObj.eventStreamName) != null) {
			// stream exists..update required
			Integer status = layer.update(streamObj);
			if (status <= 0) {
				streamObj = null;
			}
		} else {
			// stream does not exists...create required
			newObj = layer.create(streamObj);
			streamObj = transformer.convert(newObj);
		}
		return logger.traceExit(streamObj);
	}

	/**
	 * Method to Read Event Stream
	 * 
	 * @param streamName
	 *            Event Stream Name
	 * @return Event DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static EventStreamDTO findStream(String streamName) throws SQLException {
		logger.traceEntry(streamName);
		ObjectConverter transformer = new ObjectConverter();
		ManagerDAL layer = new ManagerDAL();
		EventStreamDTO dtoObject = transformer.convert(layer.readEventStream(streamName));
		return logger.traceExit(dtoObject);
	}

	/**
	 * Method to Remove Event Stream
	 * 
	 * @param streamName
	 *            Event Stream Name
	 * @return 1(true)
	 * @throws SQLException
	 *             SQLException
	 */
	public static Integer removeStream(String streamName) throws SQLException {
		logger.traceEntry(streamName);
		ManagerDAL layer = new ManagerDAL();
		Integer status = layer.deleteEventStream(streamName);
		return logger.traceExit(status);
	}

	/**
	 * Method to Upsert Context
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Context DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ProcessContextDTO upsertContext(String jsonRequest) throws SQLException {
		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		ProcessContextDTO persistanceObj = gson.fromJson(jsonRequest, ProcessContextDTO.class);
		ObjectConverter transformer = new ObjectConverter();
		ProcessContext newObj = transformer.convert(persistanceObj);
		ManagerDAL layer = new ManagerDAL();

		if (layer.readProcessContext(newObj.processContextName) != null) {
			// process context exists..update required
			Integer status = layer.update(persistanceObj);
			if (status <= 0) {
				persistanceObj = null;
			}
		} else {
			// process context does not exist..insert required
			newObj = layer.create(persistanceObj);
			persistanceObj = transformer.convert(newObj);
		}
		return logger.traceExit(persistanceObj);
	}

	/**
	 * Method to Read Context
	 * 
	 * @param contextName
	 *            Context Name
	 * @return Context DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ProcessContextDTO findContext(String contextName) throws SQLException {
		logger.traceEntry(contextName);
		ObjectConverter transformer = new ObjectConverter();
		ManagerDAL layer = new ManagerDAL();
		ProcessContextDTO dtoObject = transformer.convert(layer.readProcessContext(contextName));
		return logger.traceExit(dtoObject);
	}

	/**
	 * Method to Remove Context
	 * 
	 * @param contextName
	 *            Context Name
	 * @return 1(true)
	 * @throws SQLException
	 *             SQLException
	 */
	public static int removeContext(String contextName) throws SQLException {
		logger.traceEntry(contextName);
		ManagerDAL layer = new ManagerDAL();
		Integer status = layer.deleteProcessContext(contextName);
		return logger.traceExit(status);
	}

	/**
	 * Method to Upsert Mapping
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Mapping DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ContextMappingDefinitionDTO upsertStreamMapping(String jsonRequest) throws SQLException {
		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		ContextMappingDefinitionDTO input = gson.fromJson(jsonRequest, ContextMappingDefinitionDTO.class);
		ManagerDAL layer = new ManagerDAL();

		for (ContextMappingDTO contextMapping : input.contextMappings) {
			if (contextMapping.primaryStream.booleanValue() == false
					&& (contextMapping.joinConditions == null || contextMapping.joinConditions.isEmpty())) {
				logger.error("Please provide the join condition for the non primary stream.");
				throw new SQLException();
			}
		}

		layer.deleteContextMapping(input.stream.eventStreamName);

		for (ContextMappingDTO contextMapping : input.contextMappings) {
			contextMapping.stream = input.stream;
			layer.create(contextMapping);
		}

		return logger.traceExit(input);
	}

	/**
	 * Method to Read Mapping
	 * 
	 * @param streamName
	 *            Event Stream Name
	 * @return Mapping DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ContextMappingDefinitionDTO findStreamMapping(String streamName) throws SQLException {
		logger.traceEntry(streamName);
		ManagerDAL layer = new ManagerDAL();
		ContextMappingDefinitionDTO dtoObject = layer.readContextMapping(streamName);
		return logger.traceExit(dtoObject);
	}

	/**
	 * Method to Remove Mapping
	 * 
	 * @param streamName
	 *            Event Stream Name
	 * @return 1(true)
	 * @throws SQLException
	 *             SQLException
	 */
	public static int removeStreamMapping(String streamName) throws SQLException {
		logger.traceEntry(streamName);
		ManagerDAL layer = new ManagerDAL();
		Integer status = layer.deleteContextMapping(streamName);
		return logger.traceExit(status);
	}

	/**
	 * Method to Upsert Granular Mapping
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Mapping DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ContextMappingDefinitionDTO granularMappingUpdate(String jsonRequest) throws SQLException {
		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		ContextMappingDefinitionDTO input = gson.fromJson(jsonRequest, ContextMappingDefinitionDTO.class);
		ManagerDAL layer = new ManagerDAL();

		for (ContextMappingDTO contextMappingDto : input.contextMappings) {
			layer.update(contextMappingDto, input.stream.eventStreamName);
		}

		return logger.traceExit(findStreamMapping(input.stream.eventStreamName));
	}

	/**
	 * Method to Read Granular Mapping
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Mapping DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ContextMappingDefinitionDTO granularMappingDelete(String jsonRequest) throws SQLException {
		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		ContextMappingDefinitionDTO input = gson.fromJson(jsonRequest, ContextMappingDefinitionDTO.class);
		ManagerDAL layer = new ManagerDAL();

		for (ContextMappingDTO contextMappingDto : input.contextMappings) {
			layer.delete(contextMappingDto, input.stream.eventStreamName);
		}

		return logger.traceExit(findStreamMapping(input.stream.eventStreamName));
	}

	/**
	 * Read all data using DAO into an object of GlobalConfigurationDTO and
	 * Serialize to JSON
	 * 
	 * @return Metadata and Configuration Details
	 * @throws SQLException
	 *             SQLException
	 */
	public static String executeStream() throws SQLException {
		logger.traceEntry();
		Gson gson = new Gson();
		ManagerDAL layer = new ManagerDAL();
		GlobalConfigurationDTO configuration = layer.generateConfiguration();
		String configJson = null;
		if (configuration != null) {
			configJson = gson.toJson(configuration).toString();
			logger.info("Config object :" + configJson);

			JedisCommands jedis = RedisConnection.getConnection();
			String key = "global-config";
			Map<String, String> map = new HashMap<>();
			map.put("configuration-replica-1", configJson);
			jedis.hmset(key, map);

			List<ConfigurationDetails> configurationDetails = layer.generateOutputConfiguration();

			for (String outputConn : jedis.hkeys("output_target")) {
				jedis.hdel("output_target", outputConn);
			}

			HashMap<String, String> outputMap = new HashMap<String, String>();
			for (ConfigurationDetails configurationDetails2 : configurationDetails) {
				if (outputMap.containsKey(configurationDetails2.streamName))
					outputMap.put(configurationDetails2.streamName, outputMap.get(configurationDetails2.streamName)
							+ "," + configurationDetails2.configurationName);
				else
					outputMap.put(configurationDetails2.streamName, configurationDetails2.configurationName);
			}

			if (!outputMap.isEmpty())
				jedis.hmset("output_target", outputMap);

			jedis.del("trapaction");

			Map<String, List<Trap>> trapsMap = new HashMap<String, List<Trap>>();

			List<Trap> traps = layer.getTrapActionConfiguration();
			for (Trap trap : traps) {

				if (!trapsMap.containsKey(trap.pluginType + ":" + trap.pluginPoint)) {
					List<Trap> trapsList = new ArrayList<Trap>();
					trapsMap.put(trap.pluginType + ":" + trap.pluginPoint, trapsList);
				}
				trapsMap.get(trap.pluginType + ":" + trap.pluginPoint).add(trap);
			}

			for (String trapKey : trapsMap.keySet()) {
				jedis.hset("trapaction", trapKey, gson.toJson(trapsMap.get(trapKey)));
			}

			Map<String, String> kafkaMap = new HashMap<String, String>();
			kafkaMap.put("MetaDataBrokerList", AppConfig.metaDataBrokerList);
			kafkaMap.put("Topic", AppConfig.topic);
			jedis.hmset("Kafka", kafkaMap);
			
			logger.debug("Configuration updated in Redis");
		}
		return logger.traceExit(configJson);
	}

	/**
	 * Rest Post Call to Start Spark Execution
	 * 
	 * @return Rest Post Call
	 * @throws Exception
	 *             Exception
	 */
	public static String startSparkExecution() throws Exception {

		logger.traceEntry();
		return logger
				.traceExit(restCall("http://" + AppConfig.agentHost + ":" + AppConfig.agentPort + "/startspark", ""));
	}

	/**
	 * Rest Post Call to Stop Spark Execution
	 * 
	 * @return Rest Post Call
	 * @throws Exception
	 *             Exception
	 */
	public static String stopSparkExecution() throws Exception {

		logger.traceEntry();
		return logger
				.traceExit(restCall("http://" + AppConfig.agentHost + ":" + AppConfig.agentPort + "/stopspark", ""));
	}

	private static String restCall(String getRequestUrl, String requestBody) throws IOException {

		logger.traceEntry();
		String output = "";
		URL url = new URL(getRequestUrl);
		HttpURLConnection conn = null;
		OutputStream os = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			os = conn.getOutputStream();
			os.write(requestBody.getBytes());
			os.flush();

			if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED
					&& conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
				String tempop;
				logger.debug("Output from Server .... \n");
				while ((tempop = br.readLine()) != null) {
					logger.debug(tempop);
					output = output + tempop;
				}
			}
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
			if (os != null) {
				os.close();
			}
		}

		return logger.traceExit(output);
	}

	/**
	 * Method to Upsert Connection Details
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Connection DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ConfigurationDetailsDTO upsertConfDetails(String jsonRequest) throws SQLException {
		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		ConfigurationDetailsDTO confObj = gson.fromJson(jsonRequest, ConfigurationDetailsDTO.class);
		ObjectConverter transformer = new ObjectConverter();
		ManagerDAL layer = new ManagerDAL();
		ConfigurationDetails newObj = transformer.convert(confObj);

		if (layer.readConfigDetails(newObj.configurationName) != null) {
			// configuration exists...update required
			Integer status = layer.update(confObj);
			if (status <= 0) {
				confObj = null;
			}
		} else {
			// configuration does not exists...create required
			newObj = layer.create(confObj);
			confObj = transformer.convert(newObj);
		}

		return logger.traceExit(confObj);
	}

	/**
	 * Method to Read Connection Details
	 * 
	 * @param connName
	 *            Connection Name
	 * @return Connection DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static ConfigurationDetailsDTO findConfDetails(String connName) throws SQLException {
		logger.traceEntry(connName);
		ObjectConverter transformer = new ObjectConverter();
		ManagerDAL layer = new ManagerDAL();
		ConfigurationDetailsDTO dtoObject = transformer.convert(layer.readConfigDetails(connName));
		return logger.traceExit(dtoObject);
	}

	/**
	 * Method to Remove Connection Details
	 * 
	 * @param connName
	 *            Connection Name
	 * @return Connection DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static Integer removeConfDetails(String connName) throws SQLException {
		logger.traceEntry(connName);
		ManagerDAL layer = new ManagerDAL();
		Integer status = layer.deleteConfDetails(connName);
		return logger.traceExit(status);
	}

	/**
	 * Method to start Input Connection
	 * 
	 * @param connName
	 *            Connection Name
	 * @return Starting Input Thread/No Configuration Found
	 * @throws SQLException
	 *             SQLException
	 */
	public static String startInputOperation(String connName) throws SQLException {
		logger.traceEntry(connName);
		ManagerDAL layer = new ManagerDAL();
		ConfigurationDetails confObj = layer.readConfigDetails(connName);
		if (confObj == null) {
			logger.debug("Exiting Manager.startInputOperation() - No Configuration Found.");
			return logger.traceExit("No Configuration Found");
		} else {
			Gson gson = new Gson();
			ObjectConverter transformer = new ObjectConverter();
			ConfigurationDetailsDTO dtoObject = transformer.convert(confObj);
			logger.debug("Manager.startInputOperation() - Calling Library for starting Input Thread ");
			return logger.traceExit(new InputAgent().startInputAgent(gson.toJson(dtoObject)));
		}
	}

	/**
	 * Method to start Output Connection
	 * 
	 * @param connName
	 *            Connection Name
	 * @return Starting Output Thread/No Configuration Found
	 * @throws SQLException
	 *             SQLException
	 */
	public static String startOutputOperation(String connName) throws Exception {
		logger.traceEntry(connName);
		ManagerDAL layer = new ManagerDAL();
		ConfigurationDetails confObj = layer.readConfigDetails(connName);
		if (confObj == null) {
			logger.debug("Exiting Manager.startOutputOperation() - No Configuration Found.");
			return logger.traceExit("No Configuration Found");
		} else {
			Gson gson = new Gson();
			ObjectConverter transformer = new ObjectConverter();
			ConfigurationDetailsDTO dtoObject = transformer.convert(confObj);
			logger.debug("Manager.startOutputOperation() - Calling Library for starting Output Thread ");
			return logger.traceExit(new OutputAgent().startOutputAgent(gson.toJson(dtoObject)));
		}
	}

	/**
	 * Method to Stop Input Connection
	 * 
	 * @param connName
	 *            Input Connection Name
	 * @return Successful/Unsuccessful Message
	 */
	public static String stopInputOperation(String connName) {
		logger.traceEntry(connName);
		return logger.traceExit(new InputAgent().stopInputAgent(connName));
	}

	/**
	 * Method to Stop Output Connection
	 * 
	 * @param connName
	 *            Output Connection Name
	 * @return Successful/Unsuccessful Message
	 */
	public static String stopOutputOperation(String connName) {
		logger.traceEntry(connName);
		return logger.traceExit(new OutputAgent().stopOutputAgent(connName));
	}

	/**
	 * Method to Upsert Trap Action
	 * 
	 * @param jsonRequest
	 *            Json
	 * @return Trap DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static TrapDTO upsertTrapAction(String jsonRequest) throws SQLException {

		logger.traceEntry(jsonRequest);
		Gson gson = new Gson();
		TrapDTO trapDto = gson.fromJson(jsonRequest, TrapDTO.class);
		ObjectConverter transformer = new ObjectConverter();
		Trap trap = transformer.convert(trapDto);
		ManagerDAL layer = new ManagerDAL();

		if (layer.readConfigDetails(trap.pluginPoint) != null) {

			if (layer.readTrapAction(trap.pluginType, trap.pluginPoint, trap.condition).size() == 1) {
				layer.delete(trap.pluginType, trap.pluginPoint, trap.condition);
			}
			trap = layer.create(trapDto);
			trapDto = transformer.convert(trap);
		} else {
			logger.error("No Connection present with this name - ", trap.pluginPoint);
			throw new SQLException();
		}
		return logger.traceExit(trapDto);
	}

	/**
	 * Method to Read Trap Action
	 * 
	 * @param pluginType
	 *            ip/op
	 * @param pluginPoint
	 *            Connection Name
	 * @return List of Trap Action DTO
	 * @throws SQLException
	 *             SQLException
	 */
	public static List<TrapDTO> findTrapAction(String pluginType, String pluginPoint) throws SQLException {
		logger.traceEntry(pluginType, pluginPoint);
		ObjectConverter transformer = new ObjectConverter();
		ManagerDAL layer = new ManagerDAL();
		List<TrapDTO> dtoObject = transformer.convertTrapList(layer.readTrapAction(pluginType, pluginPoint, null));
		return logger.traceExit(dtoObject);
	}

	/**
	 * Method to Delete Trap Action
	 * 
	 * @param pluginType
	 *            ip/op
	 * @param pluginPoint
	 *            Connection Name
	 * @return 1(true)
	 * @throws SQLException
	 *             SQLException
	 */
	public static Integer removeTrapAction(String pluginType, String pluginPoint) throws SQLException {
		logger.traceEntry(pluginType, pluginPoint);
		ManagerDAL layer = new ManagerDAL();
		Integer status = layer.delete(pluginType, pluginPoint, null);
		return logger.traceExit(status);
	}

	private static void startAllInputSources() throws Exception {
		if (Checkpoint.getProperty(Constants.IP_CONN_NAMES_CHECKPOINT) != null) {
			for (String inputConnName : Checkpoint.getProperty(Constants.IP_CONN_NAMES_CHECKPOINT).split(",")) {
				try {
					Manager.startInputOperation(inputConnName);
				} catch (Exception e) {
					logger.error("Unable to start input thread " + inputConnName, e);
				}
			}
		}
	}

	private static void startAllOutputSources() throws Exception {
		if (Checkpoint.getProperty(Constants.OP_CONN_NAMES_CHECKPOINT) != null) {
			for (String outputConnName : Checkpoint.getProperty(Constants.OP_CONN_NAMES_CHECKPOINT).split(",")) {
				try {
					Manager.startOutputOperation(outputConnName);
				} catch (Exception e) {
					logger.error("Unable to start output thread " + outputConnName, e);
				}
			}
		}
	}

	public static void startAllSources() throws Exception {
		startAllInputSources();
		startAllOutputSources();
	}
}