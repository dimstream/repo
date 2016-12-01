package com.vmware.dim.agent;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.vmware.common.checkpoint.Checkpoint;
import com.vmware.common.conf.AppConfig;
import com.vmware.common.constants.Constants;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;
import com.vmware.dim.input.InputStream;

/**
 * Class to Manage life cycle (start and stop) of the input threads
 * 
 * @author ghimanshu
 *
 */
public class InputAgent {

	private static final Logger logger = LogManager.getLogger(InputAgent.class);
	private static Map<String, InputStream> map = new HashMap<String, InputStream>();

	/**
	 * Method to stop the input thread
	 * 
	 * @param connName Input Connection Name
	 * @return Successful or Unsuccessful Message
	 */
	public String stopInputAgent(String connName) {

		logger.traceEntry(connName);

		if (map.containsKey(connName)) {
			try {
				InputStream inputStream = map.get(connName);
				inputStream.stop();
				map.remove(connName);
				Checkpoint.update(Constants.IP_CONN_NAMES_CHECKPOINT, map.keySet());
			} catch (Exception e) {
				logger.error("Unable to Stop input thread - " + connName, e);
				return "Unable to Stop input thread - " + connName;
			}
		} else {
			return logger.traceExit("Unable to found any running Input Thread with the connection Name - " + connName);
		}
		return logger.traceExit("Input thread Stopped successfully - " + connName);
	}
	
	/**
	 * Method to start the input thread
	 * 
	 * @param confDetailsDTO ConnectionDetailsDTO
	 */
	public String startInputAgent(String confDetailsDTO) {

		logger.traceEntry();
		final ConfigurationDetailsDTO pConfig;
		Gson gson = new Gson();
		pConfig = gson.fromJson(confDetailsDTO, ConfigurationDetailsDTO.class);

		KafkaCoordinatesDTO kafkaCoordinates = new KafkaCoordinatesDTO();
		kafkaCoordinates.keySerializerClass = AppConfig.keySerializerClass;
		kafkaCoordinates.metaDataBrokerList = AppConfig.metaDataBrokerList;
		kafkaCoordinates.serializerClass = AppConfig.keySerializerClass;
		kafkaCoordinates.topic = AppConfig.topic;

		String inputMessage ="";
		Runnable r;
		try {
			r = InputAgents.valueOf(pConfig.configurationType).getInputStreamClass()
					.getConstructor(ConfigurationDetailsDTO.class, KafkaCoordinatesDTO.class)
					.newInstance(pConfig, kafkaCoordinates);

			Thread t = new Thread(r);
			if (map.containsKey(pConfig.configurationName)) {
				inputMessage = "Input thread is already running - " + pConfig.configurationName + ". In order to restart, please stop and then start.";
			} else {
				map.put(pConfig.configurationName, (InputStream) r);
				Checkpoint.update(Constants.IP_CONN_NAMES_CHECKPOINT, map.keySet());
				t.start();
				inputMessage = "New Input Thread started - " + pConfig.configurationName;
			}
			
		} catch (Exception e) {
			logger.error("Unable to start the Input thread - " + pConfig.configurationName, e);
			inputMessage = "Unable to start the Input thread - " + pConfig.configurationName;
		}
		return logger.traceExit(inputMessage);
	}
}