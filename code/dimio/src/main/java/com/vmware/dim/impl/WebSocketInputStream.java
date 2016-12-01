package com.vmware.dim.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.JsonPath;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;

/**
 * Web Socket input source class implementing {@link InputStreamReader}<br>
 * Continuously monitor and pushes data to DIM's Kafka DataBus
 * 
 * @author ghimanshu
 *
 */
public class WebSocketInputStream extends com.vmware.dim.input.InputStreamReader {

	private ServerSocket serverSocket;
	private ExecutorService executorService;
	private String messageKey;
	private BufferedReader in = null;
	private Socket clientSocket = null;

	private static final Logger logger = LogManager.getLogger(WebSocketInputStream.class);

	public WebSocketInputStream(ConfigurationDetailsDTO confDetailsDTO, KafkaCoordinatesDTO kafkaCoordinates)
			throws Exception {
		super(confDetailsDTO, kafkaCoordinates);
		logger.traceEntry();

		int port = 0;

		Iterator<ConfigurationPropertiesDTO> dtoIterator = confDetailsDTO.configurationProperties.iterator();
		while (dtoIterator.hasNext()) {
			ConfigurationPropertiesDTO pDTO = (ConfigurationPropertiesDTO) dtoIterator.next();
			if ("port".equals(pDTO.configurationKey))
				port = Integer.valueOf(pDTO.configurationValue);
			else if ("messageKey".equals(pDTO.configurationKey))
				messageKey = pDTO.configurationValue;
		}

		serverSocket = new ServerSocket(port);
	}

	private void acceptConnection(ServerSocket serverSocket, ExecutorService executorService) throws IOException {
		logger.traceEntry();
		clientSocket = serverSocket.accept();
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

		String inputLine;
		while ((inputLine = in.readLine()) != null) {
			String payload = inputLine;
			if (messageKey != null && !messageKey.isEmpty()) {
				payload = JsonPath.read(payload, "$." + messageKey).toString();
			}
			execute(payload);
		}
		acceptConnection(serverSocket, executorService);
		logger.traceExit();
	}

	/**
	 * Method to read data from Web Socket
	 */
	@Override
	public void run() {
		logger.traceEntry();
		try {
			logger.debug("Inside thread.. Running");
			acceptConnection(serverSocket, executorService);
		} catch (IOException e) {
			logger.error("Error accepting connection", e);
		}
		logger.traceExit();
	}

	/**
	 * Method to stop the Web Socket Input thread
	 */
	@Override
	public void stop() throws Exception {
		if (clientSocket != null) {
			clientSocket.close();
			serverSocket.close();
		}
	}	
}