package com.vmware.common.dim;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.vmware.common.conn.RedisConnection;
import com.vmware.common.constants.Constants;
import com.vmware.common.dto.ContextMappingOutputDTO;
import com.vmware.common.dto.EventStreamDTO;
import com.vmware.common.dto.GlobalConfigurationDTO;
import com.vmware.common.dto.IdentifierDTO;
import com.vmware.common.dto.StreamConfigurationDTO;
import com.vmware.common.conn.KakfaConnection;

import redis.clients.jedis.JedisCommands;

public class DataTransformationUtil {

	private static final Logger logger = LogManager.getLogger(DataTransformationUtil.class);

	public static String getDocumentId(EventStreamDTO eventStreamDTO, DocumentContext streamDocumentContext) {

		String documentId = "";
		for (IdentifierDTO identifierDTO : eventStreamDTO.identifier) {
			documentId = documentId + ScriptEngineCache.read(streamDocumentContext, identifierDTO.field) + "|";
		}
		return documentId;
	}
	
	public static void evaluateLateJoin(DocumentContext streamDocumentContext, String joinCondition, String eventStreamName)throws SQLException{
		
		List<String> joinFields = new ArrayList<String>();
		List<String> joinValues = new ArrayList<String>();

		evaluateJoinCondition(joinCondition, streamDocumentContext, joinFields, joinValues);
		
		JedisCommands jedis = RedisConnection.getConnection();
		
		for (int i = 0; i < joinFields.size(); i++) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("json", streamDocumentContext.jsonString());
			jedis.hmset(Constants.LATE_JOIN_PREFIX + eventStreamName + "_" + joinFields.get(i) + "_" + joinValues.get(i), map);
		}
	}

	public static void persistContext(DocumentContext contextDocumentContext, String documentId,
			ContextMappingOutputDTO contextMappingOutputDTO) throws Exception {

		JedisCommands jedis = RedisConnection.getConnection();
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
		df.setTimeZone(tz);
		String nowAsISO = df.format(new Date());

		contextDocumentContext.set("key", documentId);
		contextDocumentContext.set("LastProcessTime", nowAsISO);

		String key = "Context_" + contextMappingOutputDTO.context.processContextName + "_" + documentId;
		String field = "json";
		jedis.hset(key, field, contextDocumentContext.jsonString());
		
		if (contextMappingOutputDTO.context.evictionTime == null) {
			contextMappingOutputDTO.context.evictionTime = -1;
		}

		String enrichmentCompletionCondition = contextMappingOutputDTO.context.enrichmentCompletion;
		boolean evaluationConditionCheck = true;
		if (enrichmentCompletionCondition != null && !enrichmentCompletionCondition.isEmpty()) {
			try {
				evaluationConditionCheck = (boolean) ScriptEngineCache.get().evalExpr(contextDocumentContext,
						enrichmentCompletionCondition);
				logger.debug("Enrichment Evaluation condition stands to " + enrichmentCompletionCondition);
				
			} catch(Exception e) {
				evaluationConditionCheck = false;
				logger.debug("Enrichment Evaluation condition stands to False");
			}
		}
		
		if (evaluationConditionCheck) {
			setOutputConnName(contextMappingOutputDTO.context.processContextName, documentId, "Context");
			
			logger.info("evictionTime = " + contextMappingOutputDTO.context.evictionTime);
			if (contextMappingOutputDTO.context.evictionTime >= 0) {
				logger.info("Setting eviction time of " + contextMappingOutputDTO.context.evictionTime + " seconds for context " +  contextMappingOutputDTO.context.processContextName);
				jedis.expire(key, contextMappingOutputDTO.context.evictionTime);
			}
		}
	}

	public static void persistStream(DocumentContext streamDocumentContext, String documentId,
			EventStreamDTO eventStreamDTO) throws Exception {
		logger.debug(System.currentTimeMillis() + " - " + "Starting to persist individual stream data to cache");

		JedisCommands jedis = RedisConnection.getConnection();

		if (eventStreamDTO.evictionTime == null) {
			eventStreamDTO.evictionTime = -1;
		}

		String key = "Stream_" + eventStreamDTO.eventStreamName + "_" + documentId;
		String property = "json";

		Map<String, String> map = new HashMap<String, String>();
		map.put(property, streamDocumentContext.jsonString());

		jedis.hmset(key, map);
		logger.info("evictionTime = " + eventStreamDTO.evictionTime);
		if (eventStreamDTO.evictionTime >= 0) {
			logger.info("Setting eviction time of " + eventStreamDTO.evictionTime + " seconds for event stream " + eventStreamDTO.eventStreamName);
			jedis.expire(key, eventStreamDTO.evictionTime);
		}

		logger.debug("Stream Persisted");
		setOutputConnName(eventStreamDTO.eventStreamName, documentId, "Stream");

		if(eventStreamDTO.lateJoinCondition !=null && !eventStreamDTO.lateJoinCondition.isEmpty()){
			String lateJoinConditions[] = eventStreamDTO.lateJoinCondition.split(",");
			for(String joinCondition: lateJoinConditions){
				
				List<String> joinFields = new ArrayList<String>();
				List<String> joinValues = new ArrayList<String>();

				evaluateJoinCondition(joinCondition, streamDocumentContext, joinFields, joinValues);
				
				//Send the json back to Kafka
				for (int i = 0; i < joinFields.size(); i++) {
					String payload = jedis.hget(Constants.LATE_JOIN_PREFIX + joinFields.get(i) + "_" + joinValues.get(i), "json");
					if(payload!=null){
						KakfaConnection.writeToKafka(payload);
					}
					jedis.hdel(Constants.LATE_JOIN_PREFIX + joinFields.get(i) + "_" + joinValues.get(i), "json");
				}
			}
		}
		
		logger.debug(System.currentTimeMillis() + " - " + " .Completed Stream Persist");
	}

	/**
	 * Stores the pk of the stream or context against the registered output
	 * connections, which will be read by the output threads in configured
	 * intervals to persist the data into target.
	 * 
	 * @param index
	 *            Stream Name
	 * @param documentId
	 *            pk of the document
	 * @param streamType
	 *            stream/context
	 * 
	 * @throws SQLException
	 *             SQLException
	 */
	private static void setOutputConnName(String index, String documentId, String streamType) throws SQLException {
		logger.traceEntry(index, documentId, streamType);
		JedisCommands jedis = RedisConnection.getConnection();
		String connNames = jedis.hget("output_target", index);
		if (connNames != null && !connNames.isEmpty()) {
			for (String connName : connNames.split(",")) {
				logger.debug("Splitting the connection names" + connName + "_Output_" + index + "," + streamType + "_"
						+ index + "_" + documentId);
				jedis.sadd(connName + "_Output_" + index, streamType + "_" + index + "_" + documentId);
			}
		}
		logger.traceExit();
	}

	public static void evaluateJoinCondition(String joinCondition, DocumentContext streamDocumentContext, List<String> joinFields, List<String> joinValues) throws SQLException{
		
		Matcher matcher = ScriptEngineCache.get().getMatcher(joinCondition);

		while (matcher.find()) {
			String field = matcher.group(0);
			if (field.startsWith("$.Context")) {
				field = field.replaceAll("\\$.Context.", "");
				joinFields.add(field);
			} else {
				joinValues.add((String) ScriptEngineCache.read(streamDocumentContext,
						DataTransformationUtil.getComputedExpression(field)));
			}
		}
	}
	
	public static DocumentContext cachelookUp(String contextName, String joinCondition,
			DocumentContext streamDocumentContext) throws Exception {

		List<String> joinFields = new ArrayList<String>();
		List<String> joinValues = new ArrayList<String>();
		
		evaluateJoinCondition(joinCondition, streamDocumentContext, joinFields, joinValues);
		
		JedisCommands jedis = RedisConnection.getConnection();

		Set<String> finalSet = new HashSet<String>();
		for (int i = 0; i < joinFields.size(); i++) {
			String key = Constants.LOOK_UP_PREFIX + contextName + "_" + joinFields.get(i) + "_" + joinValues.get(i);
			Set<String> rSet = new HashSet<String>();
			rSet = jedis.smembers(key);
			if (i == 0) {
				finalSet = rSet;
			} else {
				finalSet.retainAll(rSet);
			}
		}

		String documentId = "";
		if (finalSet.size() > 0) {
			documentId = (String) (finalSet.toArray())[0];
			logger.debug("documentId=" + documentId);
		}
		
		String key = "Context_" + contextName + "_" + documentId;
		logger.debug("key=" + key);

		String contextJson = jedis.hget(key, "json");
		if (contextJson == null || contextJson.isEmpty()) {
			contextJson = "{}";
		}
		return JsonPath.using(DataTransformation.JSON_PATH_CONFIGURATION).parse(contextJson);
	}

	public static String getFieldName(String fieldName) {
		return fieldName.substring(2, fieldName.length());
	}

	public static boolean isExprForStream(String expr) {
		return expr.contains("$.Stream");
	}

	public static String getComputedExpression(String expr) {
		if (isExprForStream(expr)) {
			return "$." + expr.substring(9, expr.length());
		}
		return "$." + expr.substring(10, expr.length());
	}

	/**
	 * Reads the Global Stream, Context and mapping information from cached
	 * memory, if not found, this method loads the data from Manager application
	 * and also builds the cache for next use.
	 * 
	 * @return an Array List of type StreamConfiguration, the whole array
	 *         containing the global stream information
	 * @throws Exception
	 *             Exception
	 */
	public static ArrayList<StreamConfigurationDTO> readGlobalStreamConfiguration() throws Exception {

		logger.traceEntry();
		GlobalConfigurationDTO confObj = new GlobalConfigurationDTO();
		JedisCommands jedis = null;
		logger.debug(System.currentTimeMillis() + " - " + "Initiating redis pool");
		try {
			jedis = RedisConnection.getConnection();
		} catch (SQLException e) {
			logger.error("Unable to retrieve redis connection", e);
		}
		logger.debug(System.currentTimeMillis() + " - " + "Redis Resource instantiated");

		// Read global config from memory
		String key = "global-config";
		String globalConfigJson = jedis.hget(key, "configuration-replica-1");

		if (globalConfigJson != null && globalConfigJson.length() > 0) {
			// means the config is cached
			logger.debug(System.currentTimeMillis() + " - " + "Cache HIT");
		}

		logger.debug(System.currentTimeMillis() + " - " + "Returning Resource to redis pool\n" + globalConfigJson);
		Gson gson = new GsonBuilder().create();
		confObj = gson.fromJson(globalConfigJson, GlobalConfigurationDTO.class);
		return logger.traceExit(confObj.globalConfig);
	}
	
	/**
	 * Looks up the inverted index based on the join condition to retrieve the context 
	 * for further enrichment.
	 * 
	 * @param index Stream Name
	 * @param joinClause Join condition to get context json
	 * @param dataPayload Stream data
	 * 
	 * @return Conetxt json if exists
	 * @throws Exception Exception
	 */
	public static String cacheLookup(String index, String joinClause, String dataPayload) throws Exception {

		logger.traceEntry(index, joinClause, dataPayload);
		// perform Lookup operations based on the join conditions
		// For n Join conditions, there will be n sets
		// based on the AND OR topology, the sets will undergo union and
		// intersection

		// Break Join conditions to Key Value pairs
		List<String> joinFields = extractContextFields(joinClause);
		logger.debug("Join fields");
		for (String s : joinFields) {
			logger.debug(s);
		}

		List<String> joinValues = evaluateJoinConditionValues(joinClause, dataPayload, joinFields);
		logger.debug("Join field values");
		for (String s : joinValues) {
			logger.debug(s);
		}
		// tokenize and get key values
		JedisCommands jedis = null;
		try {
			jedis = RedisConnection.getConnection();
		} catch (SQLException e) {
			logger.error("Unable to retrieve redis connection", e);
		}

		// Setup SET operation in form of binary tree to be computed later on
		Set<String> finalSet = new HashSet<String>();
		for (int joinItr = 0; joinItr < joinFields.size(); joinItr++) {
			String key = Constants.LOOK_UP_PREFIX + index + "_" + joinFields.get(joinItr) + "_" + joinValues.get(joinItr);
			logger.debug("key" + key);
			Set<String> rSet = jedis.smembers(key);
			logger.debug(rSet);
			if (joinItr == 0) {
				finalSet = rSet;
			} else {
				finalSet.retainAll(rSet);
			}
		}

		// if the final set contains more than 1 values, then choose top 1
		String documentId = "";
		if (finalSet.size() > 0) {
			documentId = (String) (finalSet.toArray())[0];
			logger.debug("documentId=" + documentId);
		} else {
			// do something here to return with grace
			logger.debug("Returning");
			return "";
		}

		// once the document id is isolated, retrieve the context json
		String key = "Context_" + index + "_" + documentId;
		logger.debug("key=" + key);
		String result = jedis.hget(key, "json");
		return logger.traceExit(result);
	}
	
	/**
	 *  Process the expression condition and retrieves the list of variables to be replaced 
	 *  in the condition.
	 *  
	 * @param condition Join condition
	 * 
	 * @return List of variables in the expression
	 */
	public static List<String> extractContextFields(String condition) {

		logger.traceEntry(condition);
		List<String> variables = new ArrayList<String>();
		// Extract All fields from condition
		Scanner sc = new Scanner(condition);
		for (String s; (s = sc.findWithinHorizon("(?<=\\<).*?(?=\\>)", 0)) != null;) {
			if (s.startsWith("Context.")) {
				// remove context prefix
				s = s.replace("Context.", "");
				variables.add(s);
			}
		}
		// remove duplicate entries
		Set<String> hs = new HashSet<String>();
		hs.addAll(variables);
		variables.clear();
		variables.addAll(hs);
		sc.close();
		return logger.traceExit(variables);
	}
	
	/**
	 * Processes the join condition and extracts the variables and values to be used to retrieve the 
	 * context json with the help of inverted index from Redis
	 * 
	 * @param joinConditions expression 
	 * @param streamJson Stream Data
	 * @param contextFields Variables to be used in Join condition
	 * @return list of values to be used in the expression
	 * @throws ScriptException ScriptException
	 */
	public static List<String> evaluateJoinConditionValues(String joinConditions, String streamJson, List<String> contextFields) throws ScriptException {
		// read through the join condition to find equations, then extract
		// values, treat all conditions to be delimited by comma
		// convert all Stream fields to values
		logger.traceEntry(joinConditions, streamJson, String.valueOf(contextFields.size()));

		List<String> streamTokens = new ArrayList<String>();
		List<String> contextValues = new ArrayList<String>();
		// Extract All fields from condition
		Scanner sc = new Scanner(joinConditions);
		for (String s; (s = sc.findWithinHorizon("(?<=\\<).*?(?=\\>)", 0)) != null;) {
			if (s.startsWith("Stream.")) {
				streamTokens.add(s);
			}
		}
		// remove duplicate entries
		Set<String> hs2 = new HashSet<String>();
		hs2.addAll(streamTokens);
		streamTokens.clear();
		streamTokens.addAll(hs2);

		// extract values from json and put back in condition
		for (int i = 0; i < streamTokens.size(); i++) {
			String val = JsonPath.parse(streamJson).read("$." + streamTokens.get(i).replace("Stream.", ""), String.class);
			joinConditions = joinConditions.replace(("<" + streamTokens.get(i) + ">"), "'" + val + "'");
		}

		for (int i = 0; i < contextFields.size(); i++) {
			// replcae context tokens with javascript variables
			joinConditions = joinConditions.replace(("<Context." + contextFields.get(i) + ">"), "var variable" + i);
		}
		
		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine jsEngine = mgr.getEngineByName("JavaScript");
		jsEngine.eval(joinConditions);
		
		for (int i = 0; i < contextFields.size(); i++) {
			// evaluate the value of this field
			String value = (String) jsEngine.get("variable" + i);
			contextValues.add(i, value);
		}
		sc.close();
		return logger.traceExit(contextValues);
	}
}