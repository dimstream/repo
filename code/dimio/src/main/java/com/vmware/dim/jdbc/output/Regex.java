package com.vmware.dim.jdbc.output;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.DocumentContext;

public class Regex {

	private static final Logger logger = LogManager.getLogger(Regex.class);
	
	public static String BIND_VALUE_REGEX_PATTERN = "[$.]+[.\\w\\[\\]\\*]{0,}";
	public static Pattern BIND_VALUE_REGEX = Pattern.compile(BIND_VALUE_REGEX_PATTERN);

	public static List<String> getBindValues(String expr) {
		logger.traceEntry();
		List<String> list = new ArrayList<String>();
		Matcher matcher = BIND_VALUE_REGEX.matcher(expr);
		while (matcher.find()) {
			list.add(matcher.group(0));
		}
		return logger.traceExit(list);
	}
	
	public static String replace(String expr, DocumentContext documentContext){
		logger.traceEntry();
		Matcher matcher = BIND_VALUE_REGEX.matcher(expr);
		while (matcher.find()) {
			expr = expr.replaceAll("\\"+matcher.group(0), documentContext.read(matcher.group(0)).toString());
		}
		return logger.traceExit(expr);
	}
}
