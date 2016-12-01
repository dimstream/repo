package com.vmware.dim.agent;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.vmware.common.checkpoint.Checkpoint;
import com.vmware.common.constants.Constants;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * Class to Manage life cycle (start and stop) of the output threads
 * 
 * @author ghimanshu
 *
 */
public class OutputAgent {

	private static final Logger logger = LogManager.getLogger(OutputAgent.class);
	private static Map<String, OutputStreamWriter> map = new HashMap<String, OutputStreamWriter>();

	/**
	 * Method to stop the output thread
	 * 
	 * @param connName Output Connection Name
	 * @return Successful or Unsuccessful Message
	 */
	public String stopOutputAgent(String connName) {

		logger.traceEntry();
		if (map.containsKey(connName)) {
			OutputStreamWriter osw = map.get(connName);
			try {
				osw.stop();
				map.remove(connName);
				Checkpoint.update(Constants.OP_CONN_NAMES_CHECKPOINT, map.keySet());
			} catch (Exception e) {
				logger.error("Unable to Stop output thread - " + connName, e);
				return ("Unable to Stop output thread - " + connName);
			}
		} else {
			return logger.traceExit("Unable to found any running Output Thread with the connection Name - " + connName);
		}
		return logger.traceExit("Output thread Stopped successfully - " + connName);
	}

	/**
	 * Method to start the output thread
	 * 
	 * @param confDetailsDTO ConnectionDetailsDTO
	 * @throws Exception Exception
	 * @return Successful or Unsuccessful Message
	 */
	public String startOutputAgent(String confDetailsDTO) throws Exception {

		logger.traceEntry(confDetailsDTO);
		final ConfigurationDetailsDTO pConfig;
		Gson gson = new Gson();
		pConfig = gson.fromJson(confDetailsDTO, ConfigurationDetailsDTO.class);

		String outputMessage = "";
		try {
			Runnable runnable = (Runnable) OutputAgents.valueOf(pConfig.configurationType).getOutputStreamClass()
					.getConstructor(ConfigurationDetailsDTO.class).newInstance(pConfig);

			Thread thread = new Thread(runnable);
			if (map.containsKey(pConfig.configurationName)) {
				outputMessage = "Output thread is already running - " + pConfig.configurationName + ". In order to restart, please stop and then start.";
			}else{
			map.put(pConfig.configurationName, (OutputStreamWriter) runnable);
			Checkpoint.update(Constants.OP_CONN_NAMES_CHECKPOINT, map.keySet());
			thread.start();
			outputMessage = "New Output Thread started - " + pConfig.configurationName;
			}

		} catch (Exception e1) {
			logger.error("Unable to start the Output thread - " + pConfig.configurationName, e1);
			outputMessage = "Unable to start the Output thread - " + pConfig.configurationName;
		}
		return logger.traceExit(outputMessage);
	}
}