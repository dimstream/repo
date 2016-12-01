package com.vmware.dim.impl;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;
import com.vmware.common.dto.KafkaCoordinatesDTO;
import com.vmware.common.exceptions.SOQLException;
import com.vmware.common.exceptions.UnAuthorizedException;
import com.vmware.dim.input.InputStreamReader;

/**
 * SOQL input source class implementing {@link InputStreamReader}<br>
 * Polls the data from SOQL after every configured interval and pushes to DIM's Kafka DataBus
 * 
 * @author ghimanshu
 *
 */
public class SOQLInputStream extends InputStreamReader {

	public SOQLInputStream(ConfigurationDetailsDTO confDetailsDTO, KafkaCoordinatesDTO kafkaCoordinates) throws Exception {
		super(confDetailsDTO, kafkaCoordinates);
		logger.traceEntry();

		Iterator<ConfigurationPropertiesDTO> dtoIterator = confDetailsDTO.configurationProperties.iterator();
		while (dtoIterator.hasNext()) {
			ConfigurationPropertiesDTO pDTO = (ConfigurationPropertiesDTO) dtoIterator.next();
			if ("loginURL".equals(pDTO.configurationKey))
				loginURL = pDTO.configurationValue;
			else if ("query".equals(pDTO.configurationKey))
				query = pDTO.configurationValue;
			else if ("interval".equals(pDTO.configurationKey)) {
				interval = Integer.valueOf(pDTO.configurationValue);
				interval = interval * 1000;
			} else if ("initialCheckpointValue".equals(pDTO.configurationKey))
				initialCheckpointValue = pDTO.configurationValue;
			else if ("checkpointFileLocation".equals(pDTO.configurationKey))
				checkpointFileLocation = pDTO.configurationValue;
			else if ("serviceURL".equals(pDTO.configurationKey))
				serviceURL = pDTO.configurationValue;

		}
		login();
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		checkpointFile = new File(checkpointFileLocation);
		setInitialCheckpoint();
		queries = query.split(";");
		logger.traceExit();
	}

	private void setInitialCheckpoint() {
		logger.traceEntry();
		try {
			lastCheckpointValue = FileUtils.readFileToString(checkpointFile);
		} catch (Exception e) {
			logger.warn("Checkpoint file not found. New file will be created and initialCheckpointValue will be used "
					+ initialCheckpointValue);
			lastCheckpointValue = initialCheckpointValue;
		}

		if (lastCheckpointValue == null || lastCheckpointValue.isEmpty()) {
			lastCheckpointValue = initialCheckpointValue;
		}
		logger.traceExit();
	}

	/**
	 * Method to read data from SOQL
	 */
	@Override
	public void run() {
		logger.traceEntry();
		while (canRun) {
			try {
				publishData();
			} catch (Exception e) {
				logger.error("Error Processing request ", e);
			} finally {
				obeyInterval();
			}
		}
	}

	private void obeyInterval() {
		logger.traceEntry();
		try {
			Thread.sleep(interval);
		} catch (InterruptedException e) {
			canRun = false;
			logger.error("Interupted. Stopping Stread ", e);
		}
		logger.traceExit();
	}

	private String prepareQuery(String individualQuery) throws Exception {
		logger.traceEntry();
		logger.debug("Using checkpoint " + lastCheckpointValue);
		String queryURL = individualQuery;
		if (individualQuery.contains(":last_modified_timestamp")) {
			queryURL = individualQuery.replace(":last_modified_timestamp", lastCheckpointValue);
		}
		return logger.traceExit(queryURL = serviceURL + "?q=" + URLEncoder.encode(queryURL, "UTF-8"));
	}

	private void publishData() throws Exception {
		logger.traceEntry();
		String data = "";
		for (String individualQuery : queries) {
			try {
				data = invokeSOQL(individualQuery);
			} catch (UnAuthorizedException e) {
				retryAttempts++;
				if (retryAttempts > MAX_RETRY_ATTEMPTS) {
					logger.error("Tried to relogin " + MAX_RETRY_ATTEMPTS + " times without success, exiting.", e);
					throw new SOQLException(
							"Tried to relogin " + MAX_RETRY_ATTEMPTS + " times without success, exiting.", e);
				}
				login();
				data = invokeSOQL(individualQuery);
			}

			logger.debug(data);
			JSONObject jsonObject = new JSONObject(data);
			JSONArray records = jsonObject.getJSONArray("records");
			for (int i = 0; i < records.length(); i++) {
				execute(records.getJSONObject(i).toString());
			}
		}
		retryAttempts = 0;
		updateCheckpoint();
		logger.traceExit();
	}

	private void updateCheckpoint() throws IOException {
		logger.traceEntry();
		lastCheckpointValue = simpleDateFormat.format(new Date());
		logger.debug("Setting checkpoint " + lastCheckpointValue);
		FileUtils.writeStringToFile(checkpointFile, lastCheckpointValue);
		logger.traceExit();
	}

	private String invokeSOQL(String individualQuery) throws Exception {
		logger.traceEntry();
		String responseStr = null;
		try {
			responseStr = invokeGET(prepareQuery(individualQuery));
		} catch (UnAuthorizedException e) {
			logger.warn("Session Expired. Trying to login again");
		}
		return logger.traceExit(responseStr);
	}

	private String invokePOST(String url) throws Exception {
		logger.traceEntry();
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost post = new HttpPost(url);
		post.addHeader(CONTENT_TYPE_KEY, LOGIN_CONTENT_TYPE);
		CloseableHttpResponse response1 = httpclient.execute(post);
		try {
			response1 = httpclient.execute(post);
			HttpEntity entity1 = response1.getEntity();
			StringWriter stringWriter = new StringWriter();
			IOUtils.copy(entity1.getContent(), stringWriter);
			return stringWriter.toString();
		} catch (Exception e) {
			logger.error("Error processing POST login request ", e);
			throw e;
		} finally {
			response1.close();
		}
	}

	private String invokeGET(String url) throws UnAuthorizedException, ClientProtocolException, IOException {
		logger.traceEntry();
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);

		get.addHeader(CONTENT_TYPE_KEY, SOQL_CONTENT_TYPE);
		get.addHeader(AUTHORIZATION_KEY, "Bearer " + accessToken);

		CloseableHttpResponse response1 = httpclient.execute(get);
		try {
			if (response1.getStatusLine().getStatusCode() == 401) {
				throw new UnAuthorizedException("Unauthorized or Session Expired");
			}
			HttpEntity entity1 = response1.getEntity();
			StringWriter stringWriter = new StringWriter();
			IOUtils.copy(entity1.getContent(), stringWriter);
			return stringWriter.toString();
		} catch (Exception e) {
			logger.error("Error processing GET SOQL query", e);
			throw e;
		} finally {
			response1.close();
		}

	}

	private void login() throws Exception {
		logger.traceEntry();
		JSONObject jsonObject = new JSONObject(invokePOST(loginURL));
		accessToken = jsonObject.getString("access_token");
		logger.traceExit();
	}

	/**
	 * Method to stop the SOQL Input thread
	 */
	@Override
	public void stop() throws Exception {
		canRun = false;
	}

	private static final Logger logger = LogManager.getLogger(SOQLInputStream.class);

	private String loginURL;
	private String serviceURL;
	private String query;
	private int interval = 600000;
	private String initialCheckpointValue;
	private String checkpointFileLocation;
	private String[] queries;

	private String accessToken;
	private File checkpointFile;
	private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00'Z'");
	private String lastCheckpointValue;
	private boolean canRun = true;
	private int retryAttempts = 0;
	private static final int MAX_RETRY_ATTEMPTS = 3;

	private static final String LOGIN_CONTENT_TYPE = "application/json;application/x-www-form-urlencoded";
	private static final String CONTENT_TYPE_KEY = "Content-Type";
	private static final String SOQL_CONTENT_TYPE = "application/x-www-form-urlencoded";
	private static final String AUTHORIZATION_KEY = "Authorization";

}