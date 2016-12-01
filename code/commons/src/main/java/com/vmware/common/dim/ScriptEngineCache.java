package com.vmware.common.dim;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.jayway.jsonpath.DocumentContext;

/**
 * 
 * Singletom class to maintain a single instance of Script engine and evaluate the expression.
 * 
 * @author vedanthr
 */
public class ScriptEngineCache {
	
	private static ScriptEngine jsEngine = null;
	private static final ScriptEngineCache INSTANCE = new ScriptEngineCache();
	
	private final String REGEX_PATTERN = "[$][.]+[:.\\w]{0,}";
	private final Pattern pattern = Pattern.compile(REGEX_PATTERN);
	
	
	private ScriptEngineCache(){
		ScriptEngineManager mgr = new ScriptEngineManager();
		jsEngine = mgr.getEngineByName("JavaScript");	
	}
	
	public static ScriptEngineCache get(){
		return INSTANCE;
	}
	
	public ScriptEngine getScriptEngine(){
		return jsEngine;
	}
	
	public Object evalExpr(DocumentContext documentContext, String expr) throws ScriptException {
		Matcher matcher = getMatcher(expr);
		
		while(matcher.find()) {
			Object val = read(documentContext,matcher.group(0));
			expr = expr.replaceAll("\\"+matcher.group(0), val == null ? "" : val.toString());
		}
		return jsEngine.eval(expr);
	}
	
	public Object evalExprForComputedFields(DocumentContext documentContext, String expr) throws ScriptException {
		Matcher matcher = getMatcher(expr);
		
		while(matcher.find()) {
			String match = matcher.group(0);
			match = DataTransformationUtil.getComputedExpression(match);
			Object val = read(documentContext,match);
			expr = expr.replaceAll("\\"+matcher.group(0), val == null ? "": val.toString());
		}
		return jsEngine.eval(expr);
	}
	
	public static Object read(DocumentContext documentContext, String path){
		return documentContext.read(path);
	}
	
	public Matcher getMatcher(String expr){
		return pattern.matcher(expr);
	}
}
