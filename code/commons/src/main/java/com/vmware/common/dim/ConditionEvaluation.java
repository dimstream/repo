package com.vmware.common.dim;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

/**
 * Class to evaluate the enrichment completion expressions in the context 
 * 
 * @author vedanthr
 */
public class ConditionEvaluation {

	private static final Logger logger = LogManager.getLogger(ConditionEvaluation.class);

	public static final Pattern CONDITION_EVAL_REG_EXP = Pattern.compile("[a-zA-Z0-9.?@]+|\\[(.*)\\]+");

	/**
	 * Replaces the variables in expression using Jsonpath and evaluates the expression using
	 * ScriptEngine Manager.
	 * 
	 * @param condition Enrichment completion expression
	 * @param contextJson Context Json to be evaluated to enrichment completion
	 * @return true, if context is complete else returns false
	 */
	public static boolean verifyEvaluationCondition(String condition, String contextJson) {

		if (condition == null || condition.isEmpty()) {
			logger.debug("Enrichment condition is blank");
			return true;
		}
		boolean status = false;
		try {
			Matcher matcher = CONDITION_EVAL_REG_EXP.matcher(condition);
			DocumentContext documentContext = JsonPath.parse(contextJson);
			int counter = 0;
			String prev = "";
			List<String> variables = new ArrayList<String>();

			while (matcher.find()) {

				if (counter % 2 == 0) {
					variables.add(matcher.group(0));
				}

				if (matcher.group(0).contains("[?(@")) {
					variables.remove(variables.size() - 1);
					prev = prev + matcher.group(0);
					variables.add(prev);
					counter++;
				}

				prev = matcher.group(0);
				counter++;
			}
			logger.debug("ConditionEvaluation.verifyEvaluationCondition() -> variables = " + variables);

			for (String var : variables) {
				try {
					condition = condition.replace(var, "'" + documentContext.read(var).toString() + "'");
				} catch (Exception e) {
					logger.error(e);
					break;
				}
			}

			logger.debug("ConditionEvaluation.verifyEvaluationCondition() -> condition = " + condition);

			ScriptEngineManager manager = new ScriptEngineManager();
			ScriptEngine engine = manager.getEngineByName("js");
			status = ((Boolean) engine.eval(condition));
		} catch (Exception e) {
			logger.error("Error evaluating enrichment completion condition", e);
		}
		return status;
	}
}
