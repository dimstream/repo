package com.vmware.common.dim;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * Utility class to prepare a nested/aggregated context json by appending the
 * incoming stream (child document) at the defined path in context json.
 * 
 * @author vedanthr
 *
 */
public class NestedFieldWriter {

	private static final Logger logger = LogManager.getLogger(NestedFieldWriter.class);

	/**
	 * Method to append the Child document (Incoming stream) to the context 
	 * based on the given Json path
	 * 
	 * @param parent Context document to append the child josn
	 * @param child Incoming Stream which will be appended to context 
	 * @param path Json path in the context json, where the child document will be appended
	 */
	public static void updateContext(DocumentContext parent, DocumentContext child, String path) {

		logger.traceEntry(parent.toString(), child.toString(), path);
		RevIterator iterator = new RevIterator(splitPath(path, child));
		try {
			logger.debug("Trying to find full path " + iterator.getFullPath());
			if (isArray(iterator.getFullPath())) {
				Object o = parent.read(iterator.getFullPath());
				if (o == null || o.toString().equals("[]") || o.toString().equals("[null]")) {
					throw new PathNotFoundException();
				}
				logger.debug("Found an object to update");
				return;
			}
		} catch (Exception e) {
			logger.error("Path not found. Proceeding with split and find");
		}
		try {
			validateAndSetProperty(parent, iterator, child, iterator.getFullPath());
		} catch (InvalidPathException e) {
			logger.error("Invald path exception", e);
		}
		logger.traceExit();
	}

	private static void validateAndSetProperty(DocumentContext documentContext, RevIterator iterator,
			DocumentContext child, String path) throws InvalidPathException {
		logger.traceEntry(documentContext.toString(), iterator.toString(), child.toString(), path);
		String prev = iterator.prev();
		try {
			Object o = null;

			if (!prev.equals("")) {
				if (prev.endsWith("[]")) {
					prev = prev.substring(0, prev.length() - 2);
				}
				o = documentContext.read(prev);
				if (o == null || o.toString().equals("[]") || o.toString().equals("[null]")) {
					throw new PathNotFoundException();
				}
			}

			iterator.setPosition();

			if (!iterator.hasNext()) {
				addNode(documentContext, iterator, child, path);
			} else {
				while (iterator.hasNext()) {
					addNode(documentContext, iterator, child, path);
				}
			}
		} catch (PathNotFoundException e) {
			logger.error(prev + " not found, adding to the context..", e);
			validateAndSetProperty(documentContext, iterator, child, path);
		}
		logger.traceExit();
	}

	private static void addNode(DocumentContext documentContext, RevIterator iterator, DocumentContext child,
			String fullPath) throws InvalidPathException {
		logger.traceEntry(documentContext.toString(), iterator.toString(), child.toString(), fullPath);
		String path = iterator.next();
		if (path == null || path.isEmpty()) {
			path = fullPath;
		}

		if (!iterator.isLeaf() && isArray(path)) {
			throw new InvalidPathException(path);
		}

		if (!iterator.isLeaf()) {
			addParentNode(documentContext, path);
		} else {
			if (isArray(path)) {
				if (path.endsWith("[]")) {
					path = path.substring(0, path.length() - 2);
				} else {
					path = path.substring(0, path.lastIndexOf("["));
				}
				Object o = documentContext.read(path);
				if (o == null || o.toString().equals("[]") || o.toString().equals("[null]")) {
					List<Map<String, Object>> emptyList = new ArrayList<Map<String, Object>>();
					documentContext.set(path, emptyList);
				}
				documentContext.add(path, child.json());

			} else {
				documentContext = documentContext.set(path, child.json());
			}
		}
		logger.traceExit();
	}

	private static void addParentNode(DocumentContext documentContext, String path) {
		documentContext.set(path, JsonPath.parse("{}").json());
	}

	private static List<String> splitPath(String path, DocumentContext childDocumentContext) {

		logger.traceEntry(path, childDocumentContext.toString());
		String[] pathSplit = path.split("\\.");
		List<String> result = new ArrayList<String>();
		boolean append = false;
		String prevElement = "";

		for (String str : pathSplit) {
			if (str.contains("[]")) {
				append = false;
			} else if (str.contains("[")) {
				append = true;
			} else if (str.endsWith("]")) {
				append = false;
				prevElement += "." + str;
				str = prevElement;
				prevElement = "";

				String[] split = str.split("==");
				split[0] = split[0].trim();
				split[1] = split[1].trim();
				split[1] = split[1].substring(1, split[1].length() - 3);

				String value = split[1];
				try {
					value = childDocumentContext.read(split[1]);

				} catch (PathNotFoundException p) {
					logger.debug("Array Path not found " + split[1]);
				}
				str = split[0] + "=='" + value + "')]";
			}

			if (append) {
				prevElement += prevElement.equals("") ? str : "." + str;
			} else {
				result.add(str);
			}
		}
		return logger.traceExit(result);
	}

	private static boolean isArray(String path) {
		return path.endsWith("]");
	}
	
}