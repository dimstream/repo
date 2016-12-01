package com.vmware.common.trap;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.vmware.common.action.ActionInterface;
import com.vmware.common.conn.RedisConnection;
import com.vmware.common.dim.ConditionEvaluation;
import com.vmware.common.dim.DataTransformationUtil;
import com.vmware.common.dto.ActionDTO;
import com.vmware.common.dto.TrapDTO;
import com.vmware.common.enums.ActionEnum;

import redis.clients.jedis.JedisCommands;

/**
 * Class to check if any traps are defined for an incoming stream and if 
 * the stream matches any of the defined traps, the corresponding action would be 
 * applied on the stream. <br>
 * 
 * <b>Sample Configuration: </b>
 * <pre>
 * {@code
 * {
 *   "condition": "PasswordResetAttempt == '3'",
 *   "pluginType": "ip",
 *   "pluginPoint": "user_registration_rmq_conn",
 *   "actions": [{
 *       "actionId": 1,
 *       "actionType": "email",
 *       "configuration": [{
 *           "key": "Subject",
 *           "value": "New opportunities"
 *       },
 *       {
 *           "key": "Mail To",
 *           "value": "himanshug@vmware.com"
 *       }]
 *   }]
 *  }  
 * }
 * </pre>
 * 
 * @author vedanthr
 */
public class TrapAction {

	private static final Logger logger = LogManager.getLogger(TrapAction.class);

	/**
	 * 
	 * Check the availability of the key in Redis, and retrieves the 
	 * configuration if the key key exists. 
	 * 
	 * @param key Connection Name
	 * @return The trap-action configuration defined for the input key 
	 * @throws SQLException SQLException
	 */
	public static String getJsonFromRedis(String key) throws SQLException {
		logger.traceEntry();
		JedisCommands jedis = RedisConnection.getConnection();
		if (jedis.hexists("trapaction", key)) {
			return jedis.hget("trapaction", key);
		}
		return null;
	}

	/**
	 * Get the trap expression configured for the connection and
	 * validated the expression against the incoming stream.
	 * 
	 * If the expression evaluates to true, the corresponding actions will be
	 * executed sequentially.
	 * 
	 * @param key Connection name
	 * @param streamDataJson Incoming data from configured connection
	 * @return true if the stream can be passwd through
	 * @throws Exception Exception
	 */
	public static boolean trapAction(String key, String streamDataJson) throws Exception {
		logger.traceEntry(key, streamDataJson);
		boolean status = true;
		Gson gson = new Gson();
		String trapStr = getJsonFromRedis(key);
		logger.debug(trapStr);
		if (trapStr != null) {
			TrapDTO[] trapList = gson.fromJson(trapStr, TrapDTO[].class);

			for (TrapDTO trapDTO : trapList) {

				String expression = trapDTO.condition;
				// if join condition exists, get context json - streamDataJson =
				// context json

				if (trapDTO.joinCondition != null && !trapDTO.joinCondition.isEmpty()) {
					try {
						streamDataJson = DataTransformationUtil.cacheLookup(trapDTO.contextName, trapDTO.joinCondition,
								streamDataJson);
					} catch (Exception e) {
						logger.error("Trap Join Condition does not matches to the stream");
					}
				}

				if (ConditionEvaluation.verifyEvaluationCondition(expression, streamDataJson)) {
					logger.debug("Condition is true");
					for (ActionDTO actionDTO : trapDTO.actions) {
						try {
							ActionInterface actionInterface = ActionEnum.valueOf(actionDTO.actionType)
									.getActionImplementationClass().newInstance();
							if (!actionInterface.doAction(actionDTO, streamDataJson)) {
								status = false;
							}
						} catch (Exception e) {
							logger.debug("Unable to identify the Output class\n");
							logger.error("Error in trap action", e);
						}
					}
				}
			}
		}
		logger.debug("Returning status " + status);
		return status;
	}
}
