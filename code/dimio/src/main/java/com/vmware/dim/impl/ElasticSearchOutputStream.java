package com.vmware.dim.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Base64;
import org.json.JSONObject;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * Elasticsearch output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to Elasticsearch
 * 
 * @author ghimanshu
 *
 */
public class ElasticSearchOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(ElasticSearchOutputStream.class);
	private String hostName;
	private String portName;
	private String indexName;
	private String indexType;
	private String userName;
	private String password;
	private String tag;

	public ElasticSearchOutputStream(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		super(configurationDetailsDTO);
		logger.traceEntry();

		hostName = configuration.get("HostName");
		portName = configuration.get("PortName");
		indexName = configuration.get("IndexName");
		indexType = configuration.get("IndexType");
		userName = configuration.get("UserName");
		password = configuration.get("Password");
		tag = configuration.get("Tag");

		logger.traceExit();
	}

	/**
	 * Method to write data to Elasticsearch for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {
		logger.traceEntry();
		for (String name : redisData.keySet()) {
			String value = redisData.get(name);
			try {
				JSONObject jsonObject = null;
				if (tag != null) {
					jsonObject = new JSONObject(value);
					String tags[] = tag.split("\\.");
					for (int i = 1; i < tags.length; i++) {
						jsonObject = jsonObject.getJSONObject(tags[i]);
					}
					value = jsonObject.toString();
				}

				elasticSearchUpsert(value, name.substring(0,name.length()-1));
			} catch (Exception e) {
				logger.error("Error inserting data to Elasticsearch", e);
			}
		}
		logger.traceExit();
	}

	/**
	 * Method to prepare the whole document to be persisted to Elasticsearch
	 * 
	 * @param jsonDocument
	 *            Document value
	 * @param documentId
	 *            Document key
	 * @throws IOException
	 *             IOException
	 */
	public void elasticSearchUpsert(String jsonDocument, String documentId) throws IOException {

		logger.traceEntry(jsonDocument, documentId);
		String requestUrl = "http://" + hostName + ":" + portName + "/" + indexName + "/" + indexType + "/"
				+ URLEncoder.encode(documentId, "UTF-8") + "/_update";
		String queryBody = "{\n" + "    \"doc\" :" + jsonDocument + " ,\n" + "    \"doc_as_upsert\" : true\n" + "}";
		logger.debug(requestUrl);
		logger.debug(queryBody);
		String completeMessage = restCallPost(requestUrl, queryBody);
		logger.debug("Message = " + completeMessage);
		logger.traceExit();
	}

	/**
	 * Rest call to persist data to Elasticsearch
	 * 
	 * @param getRequestUrl
	 *            Complete URL
	 * @param requestBody
	 *            Complete Document
	 * @return Complete Message
	 * @throws IOException
	 *             IOException
	 */
	public String restCallPost(String getRequestUrl, String requestBody) throws IOException {
		logger.traceEntry(getRequestUrl, requestBody);
		String output = "";
		URL url = new URL(getRequestUrl);
		HttpURLConnection conn = null;
		OutputStream os = null;

		try {
			conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);

			if (userName != null && password != null) {
				String decoded = userName + ":" + password;
				String encoded = Base64.encodeBytes(decoded.getBytes());
				conn.setRequestProperty("Authorization", "Basic " + encoded);
			}

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

}