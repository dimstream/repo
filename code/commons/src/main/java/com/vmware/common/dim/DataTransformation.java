package com.vmware.common.dim;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.vmware.common.dto.StreamConfigurationDTO;
import com.vmware.common.processor.ComputedFieldsProcessor;
import com.vmware.common.processor.ExternalLookupProcessor;
import com.vmware.common.processor.PersistedFieldsProcessor;
import com.vmware.common.processor.RevisedFieldsProcessor;

/**
 * Helper class containing Abstracted Logic in form of static methods, which are
 * utilized in Classes utilized by Spark engine directly
 *
 * @author vedanthr
 */
public class DataTransformation {

	private static final Logger logger = LogManager.getLogger(DataTransformation.class);

	public static final Configuration JSON_PATH_DEFAULT_CONFIGURATION = Configuration.defaultConfiguration();
	public static final Configuration JSON_PATH_CONFIGURATION = JSON_PATH_DEFAULT_CONFIGURATION
			.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

	/**
	 * Processes the incoming streams, which involves -
	 * 
	 * Identifies the stream of incoming payload using the filter conditions.
	 * If its a primary stream, the context will be built based on the persisted and computed fields.
	 * If its a secondary stream, the context will be retrieved based in the join condition from Redis and
	 * will be enriched based on the persisted and computed fields.
	 * 
	 * Once the context is enriched, the pk of the document will be persisted against the registered output
	 * connection to be written to the target.
	 * 
	 * @param dataPayload
	 *            Stream data
	 * @throws Exception
	 *             Exception
	 */
	public static void processStreamPacket(String dataPayload) throws Exception {

		logger.traceEntry(dataPayload);
		ArrayList<StreamConfigurationDTO> globalConf = DataTransformationUtil.readGlobalStreamConfiguration();
		logger.debug(System.currentTimeMillis() + " - " + "Global Configurations retrieved");

		// Iterate through the Stream Configurations and proceed where this
		// message meets filter condition
		DocumentContext streamDocumentContext = JsonPath.using(JSON_PATH_CONFIGURATION).parse(dataPayload);
		DocumentContext contextDocumentContext = JsonPath.using(JSON_PATH_CONFIGURATION).parse("{}");

		for (int streamItr = 0; streamItr < globalConf.size(); streamItr++) {
			logger.debug(System.currentTimeMillis() + " - " + "Evaluating for Stream - " + globalConf.get(streamItr).stream.eventStreamName);
			logger.debug(System.currentTimeMillis() + " - " + "Filter - " + globalConf.get(streamItr).stream.filter);

			boolean isFilterStatisfied = false;
			try{
				isFilterStatisfied = (boolean)ScriptEngineCache.get().evalExpr(streamDocumentContext, globalConf.get(streamItr).stream.filter);
			}finally{
				if (!isFilterStatisfied) {
					logger.warn("Incoming Stream does not belong to "+ globalConf.get(streamItr).stream.eventStreamName);
					continue;
				}
			}
			
			try{
				logger.debug(System.currentTimeMillis() + " - " + "Evaluated to True. Starting with Stream Configuration");

				String documentId = DataTransformationUtil.getDocumentId(globalConf.get(streamItr).stream, streamDocumentContext);

				if(globalConf.get(streamItr).contextMappings != null){
				
					for (int contextItr = 0; contextItr < globalConf.get(streamItr).contextMappings.size(); contextItr++) {

						try{
							RevisedFieldsProcessor.process(streamDocumentContext, globalConf.get(streamItr).contextMappings.get(contextItr));
							ExternalLookupProcessor.process(streamDocumentContext, globalConf.get(streamItr).contextMappings.get(contextItr));
	
							if (!globalConf.get(streamItr).contextMappings.get(contextItr).computedStates.isEmpty() || !globalConf.get(streamItr).contextMappings.get(contextItr).persistedFields.isEmpty()) {
	
								if (!globalConf.get(streamItr).contextMappings.get(contextItr).primaryStream) {
									logger.debug(System.currentTimeMillis() + " - " + "Non primary Stream received ");
									contextDocumentContext = DataTransformationUtil.cachelookUp(globalConf.get(streamItr).contextMappings.get(contextItr).context.processContextName, globalConf.get(streamItr).contextMappings.get(contextItr).joinConditions, streamDocumentContext);
									documentId = (String) ScriptEngineCache.read(contextDocumentContext, "$.key");
								}
	
								if(documentId == null){
									DataTransformationUtil.evaluateLateJoin(streamDocumentContext, globalConf.get(streamItr).contextMappings.get(contextItr).joinConditions, globalConf.get(streamItr).stream.eventStreamName);
									continue;
								}
								
								PersistedFieldsProcessor.process(streamDocumentContext, contextDocumentContext, globalConf.get(streamItr).contextMappings.get(contextItr), documentId);
								ComputedFieldsProcessor.process(streamDocumentContext, contextDocumentContext, globalConf.get(streamItr).contextMappings.get(contextItr), documentId);
							}
							DataTransformationUtil.persistContext(contextDocumentContext, documentId, globalConf.get(streamItr).contextMappings.get(contextItr));
						
						  }catch(Exception e){
							  logger.error("Error Executing the context - " + globalConf.get(streamItr).contextMappings.get(contextItr).context.processContextName + " for stream - " + globalConf.get(streamItr).stream.eventStreamName, e);
						}
					}
				}
				if(documentId !=null){
					DataTransformationUtil.persistStream(streamDocumentContext, documentId, globalConf.get(streamItr).stream);
				}

			}catch(Exception exception){
				logger.error("Error Executing the stream - " + globalConf.get(streamItr).stream.eventStreamName);
			}
		}
	}

}