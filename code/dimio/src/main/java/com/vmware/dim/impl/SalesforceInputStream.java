package com.vmware.dim.impl;

//package demo;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.client.ClientSessionChannel.MessageListener;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;
import com.vmware.dim.input.InputStreamReader;

/**
 * Salesforce input source class implementing {@link InputStreamReader}<br>
 * Continuously monitor and pushes data to DIM's Kafka DataBus
 * 
 * @author ghimanshu
 *
 */
public class SalesforceInputStream extends InputStreamReader {

	private String loginEndpoint;
	private String userName;
	private String password;

	private String channel;
	private String streamingEndpointURI;

	private static final int CONNECTION_TIMEOUT = 20 * 1000; // milliseconds
	private static final int READ_TIMEOUT = 120 * 1000; // milliseconds

	private BayeuxClient client = null;

	private static final Logger logger = LogManager.getLogger(SalesforceInputStream.class);

	public SalesforceInputStream(ConfigurationDetailsDTO confDetailsDTO, KafkaCoordinatesDTO kafkaCoordinates) throws Exception {
		super(confDetailsDTO, kafkaCoordinates);
		logger.traceEntry();

		Iterator<ConfigurationPropertiesDTO> dtoIterator = confDetailsDTO.configurationProperties.iterator();
		while (dtoIterator.hasNext()) {
			ConfigurationPropertiesDTO pDTO = (ConfigurationPropertiesDTO) dtoIterator.next();
			if ("LoginEndpoint".equals(pDTO.configurationKey))
				loginEndpoint = pDTO.configurationValue;
			else if ("UserName".equals(pDTO.configurationKey))
				userName = pDTO.configurationValue;
			else if ("Password".equals(pDTO.configurationKey))
				password = pDTO.configurationValue;
			else if ("Channel".equals(pDTO.configurationKey))
				channel = pDTO.configurationValue;
			else if ("StreamingEndpointURI".equals(pDTO.configurationKey))
				streamingEndpointURI = pDTO.configurationValue;
		}

		logger.traceExit(channel);
	}

	/**
	 * Method to read data from Salesforce
	 */
	@Override
	public void run() {
		logger.traceEntry();
		try {
			logger.debug("Running streaming client ....");
			try{
				connectClient();	
			}catch(Exception e){
				logger.error("Error subscribing to client. Will try reconnecting ", e);
			}

			Runnable salesForceClientMonitor = new SalesForceClientMonitor(this);
			Thread thread = new Thread(salesForceClientMonitor);
			thread.start();

		} catch (Exception e) {
			logger.error("Error consuming messages - ", e);
		}
		logger.traceExit();
	}

	public void disconnectClient() throws Exception {
		logger.debug("Disconnecting Client ");
		client.getChannel(channel).unsubscribe();
		client.disconnect();
		client.abort();
		client = null;
		logger.debug("Disconnected Client ");
	}

	public void connectClient() throws Exception {
		logger.debug("Connecting Client ");
		client = makeClient();

		client.getChannel(Channel.META_HANDSHAKE).addListener(new ClientSessionChannel.MessageListener() {

			public void onMessage(ClientSessionChannel channel, Message message) {

				logger.debug("[CHANNEL:META_HANDSHAKE]: " + message);

				boolean success = message.isSuccessful();
				if (!success) {
					String error = (String) message.get("error");
					if (error != null) {
						logger.debug("Error during HANDSHAKE: " + error);
						return;
					}

					Exception exception = (Exception) message.get("exception");
					if (exception != null) {
						exception.printStackTrace();
						return;
					}
				}
			}

		});

		client.getChannel(Channel.META_CONNECT).addListener(new ClientSessionChannel.MessageListener() {
			public void onMessage(ClientSessionChannel channel, Message message) {

				logger.debug("[CHANNEL:META_CONNECT]: " + message);

				boolean success = message.isSuccessful();
				if (!success) {
					String error = (String) message.get("error");
					if (error != null) {
						logger.debug("Error during CONNECT: " + error);
						return;
					}
				}
			}

		});

		client.getChannel(Channel.META_SUBSCRIBE).addListener(new ClientSessionChannel.MessageListener() {

			public void onMessage(ClientSessionChannel channel, Message message) {

				logger.debug("[CHANNEL:META_SUBSCRIBE]: " + message);
				boolean success = message.isSuccessful();
				if (!success) {
					String error = (String) message.get("error");
					if (error != null) {
						logger.debug("Error during SUBSCRIBE: " + error);
						return;
					}
				}
			}
		});

		client.handshake();
		logger.debug("Waiting for handshake");

		boolean handshaken = client.waitFor(10 * 1000, BayeuxClient.State.CONNECTED);
		if (!handshaken) {
			logger.debug("Failed to handshake: " + client);
			throw new Exception();
		}

		logger.debug("Subscribing for channel: " + channel);

		client.getChannel(channel).subscribe(new MessageListener() {
			@Override
			public void onMessage(ClientSessionChannel channel, Message message) {
				logger.debug("Received Message: " + message);
				execute(message.toString());
			}
		});

		logger.debug("Waiting for streamed data from your organization ...");
	}

	private BayeuxClient makeClient() throws Exception {
		logger.debug("Logging into sales force");
		HttpClient httpClient = new HttpClient();
		httpClient.setConnectTimeout(CONNECTION_TIMEOUT);
		httpClient.setTimeout(READ_TIMEOUT);
		httpClient.start();

		String[] pair = SalesforceSoapLoginUtil.login(httpClient, userName, password, loginEndpoint);

		if (pair == null) {
			throw new Exception();
		}

		assert pair.length == 2;
		final String sessionid = pair[0];
		String endpoint = pair[1];
		logger.debug("Login successful!\nServer URL: " + endpoint + "\nSession ID=" + sessionid);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(ClientTransport.TIMEOUT_OPTION, READ_TIMEOUT);
		LongPollingTransport transport = new LongPollingTransport(options, httpClient) {

			@Override
			protected void customize(ContentExchange exchange) {
				super.customize(exchange);
				exchange.addRequestHeader("Authorization", "OAuth " + sessionid);
			}
		};

		BayeuxClient client = new BayeuxClient(salesforceStreamingEndpoint(endpoint), transport);
		return client;
	}

	private String salesforceStreamingEndpoint(String endpoint) throws MalformedURLException {
		return new URL(endpoint + streamingEndpointURI).toExternalForm();
	}

	/**
	 * Method to stop the Salesforce Input thread
	 */
	@Override
	public void stop() throws Exception {
		client.disconnect();
	}

	public BayeuxClient getBayeuxClient() {
		return client;
	}

	public String getChannel() {
		return channel;
	}

	public String getConnectionName(){
		return this.connName;
	}
}
