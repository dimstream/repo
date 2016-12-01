package com.vmware.common.dim;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Iterator utility class to parse the Json Path and store the individual splits.
 * This will be used while evaluating the context to append the stream json in 
 * a defined nested path.
 * 
 * @author vedanthr
 *
 */
public class RevIterator {

	private List<String> list;
	private int counter;
	private int forward;
	private static final Logger logger = LogManager.getLogger(RevIterator.class);

	public RevIterator(List<String> list) {
		logger.debug(list);
		this.list = list;
	}

	public String prev() {
		String path = "";
		boolean isFirst = true;
		for (int i = 0; i < (list.size() - counter); i++) {
			path += isFirst ? list.get(i) : "." + list.get(i);
			isFirst = false;
		}
		counter++;
		return path;
	}

	public int getCounter() {
		return counter;
	}

	public void setPosition() {
		forward = list.size() - (counter - 1);
	}

	public String next() {
		String path = "";
		boolean isFirst = true;
		if (list.size() == forward) {
			return null;
		}
		for (int i = 0; i <= forward; i++) {
			path += isFirst ? list.get(i) : "." + list.get(i);
			isFirst = false;
		}
		forward++;
		return path;
	}

	public boolean hasNext() {
		return !(list.size() == forward);
	}

	public boolean isLeaf() {
		return list.size() == forward;
	}

	public String getFullPath() {
		String parentPath = "";
		String dot = "";
		for (int i = 0; i < list.size(); i++) {
			parentPath += dot + list.get(i);
			dot = ".";
		}
		return parentPath;
	}
}
