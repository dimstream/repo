package com.vmware.dim.jdbc.output;

import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Script {

	private static final Logger logger = LogManager.getLogger(Script.class);
	private static final ScriptEngineManager SCRIPT_ENGINE_MANAGER = new ScriptEngineManager();
	private static Pattern pattern = Pattern.compile("[$=]+");
	
	public static Object eval(String condition) throws ScriptException{
		logger.traceEntry(condition);
		if(!condition.contains("(")){
			return condition;
		}
		ScriptEngine engine = SCRIPT_ENGINE_MANAGER.getEngineByName("JavaScript");
	    return logger.traceExit(engine.eval(condition));
	}
	
}
