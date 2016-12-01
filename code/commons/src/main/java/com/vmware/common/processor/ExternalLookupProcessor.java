package com.vmware.common.processor;

import java.io.StringWriter;
import java.net.URLEncoder;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.vmware.common.dim.ScriptEngineCache;
import com.vmware.common.dto.ContextMappingOutputDTO;
import com.vmware.common.dto.URLTagDTO;

public class ExternalLookupProcessor {

	private static final Logger logger = LogManager.getLogger(ExternalLookupProcessor.class);

	public static void process(DocumentContext streamDocumentContext, ContextMappingOutputDTO contextMappingOutputDTO) throws Exception {
		
		if (contextMappingOutputDTO.externalLookUp != null && contextMappingOutputDTO.externalLookUp.urlTag != null) {
			
			logger.debug(System.currentTimeMillis() + " - " + "Starting evaluation for External Look Up - Total of " + contextMappingOutputDTO.externalLookUp.urlTag.size());
			try{
				
				JSONObject jsonObject = new JSONObject(invokePOST(contextMappingOutputDTO.externalLookUp.loginURL, contextMappingOutputDTO.externalLookUp.contentTypeHeader));
				String accessToken = jsonObject.getString(contextMappingOutputDTO.externalLookUp.tokenKey);
				JSONObject tagObject = null;
				
				for (URLTagDTO urlTagDTO : contextMappingOutputDTO.externalLookUp.urlTag) {
					logger.debug(System.currentTimeMillis() + " - " + "Evaluating URL " + urlTagDTO.url + " for tag " + urlTagDTO.tag);

					try {
						String responseStr = invokeGET(urlTagDTO.url, urlTagDTO.contentTypeHeader, urlTagDTO.authorizationHeader + accessToken, urlTagDTO.queryParam, streamDocumentContext);
						tagObject = new JSONObject(responseStr);
						logger.debug("External Lookup ... tagObject-" + tagObject);
						
						if(!urlTagDTO.tag.contains("=")){
							streamDocumentContext.set(urlTagDTO.tag, JsonPath.parse(tagObject.toString()).json());	
						}else{
							setFileds(streamDocumentContext, urlTagDTO.tag, JsonPath.parse(tagObject.toString()));
						}
						
					} catch (Exception e) {
						logger.error(System.currentTimeMillis() + " - " + "Issue retreiving data from URL, continuing with null", e);
						streamDocumentContext.set(urlTagDTO.tag, "");
					}
					
				}
				
			}catch (Exception e) {
				logger.error(System.currentTimeMillis() + " - " + " Error invoking POST request - " + streamDocumentContext.jsonString() , e);
			}
		}
	}
	
	private static void setFileds(DocumentContext documentContext, String tag, DocumentContext response) throws Exception {
		String fields[] = tag.split("~");
		for(String field : fields){
			String keyVals[] = field.split("=");
			documentContext.set(keyVals[0], response.read(keyVals[1]));
		}
	}
	
	private static String invokePOST(String url, String loginContentType) throws Exception {
		logger.traceEntry(url, loginContentType);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost post = new HttpPost(url);
		post.addHeader("Content-Type", loginContentType);
		CloseableHttpResponse response1 = httpclient.execute(post);
		try {
			response1 = httpclient.execute(post);
			HttpEntity entity1 = response1.getEntity();
			StringWriter stringWriter = new StringWriter();
			IOUtils.copy(entity1.getContent(), stringWriter);
			return logger.traceExit(stringWriter.toString());
		} catch (Exception e) {
			logger.error("Error processing POST login request ", e);
			throw e;
		} finally {
			response1.close();
		}
	}

	private static String invokeGET(String url, String contentType, String accessToken, String queryParam, DocumentContext streamDocumentContext) throws Exception {
		logger.traceEntry(url, contentType, accessToken, queryParam);
		String query[] = queryParam.split(",");
		
		for (int i = 0; i < query.length; i++) {
			if(query[i].contains("[*]")){
				String val = ScriptEngineCache.read(streamDocumentContext, query[i]).toString();
				val = "("+ val.substring(1,val.length()-1).replaceAll("\"", "'") +")";
				url = url.replace(":" + (i + 1), val);
			}else{
				url = url.replace(":" + (i + 1), ScriptEngineCache.read(streamDocumentContext, query[i]).toString());	
			}
			
		}
		logger.debug("url=" + url);
		@SuppressWarnings("deprecation")
		String encodedUrl = url.replace(url.split("\\?q=")[1], URLEncoder.encode(url.split("\\?q=")[1], "UTF-8"));
		logger.debug("Encoded url=" + encodedUrl);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response1 = null;
		try {
			HttpGet get = new HttpGet(encodedUrl);
			get.addHeader("Content-Type", contentType);
			get.addHeader("Authorization", accessToken);

			response1 = httpclient.execute(get);
			if (response1.getStatusLine().getStatusCode() == 401) {
				throw new Exception("Unauthorized or Session Expired");
			}else if(response1.getStatusLine().getStatusCode() == 413){
				response1.close();
				url = url.replaceAll(" ", "+");
				logger.debug("Partial encoded URL "+url);
				get = new HttpGet(url);
				get.addHeader("Content-Type", contentType);
				get.addHeader("Authorization", accessToken);

				response1 = httpclient.execute(get);
			}
					
			HttpEntity entity1 = response1.getEntity();
			StringWriter stringWriter = new StringWriter();
			IOUtils.copy(entity1.getContent(), stringWriter);
			return logger.traceExit(stringWriter.toString());
		} catch (Exception e) {
			logger.error("Error processing GET SOQL query", e);
			throw e;
		} finally {
			response1.close();
		}
	}
	
}
