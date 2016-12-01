package com.vmware.common.action.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.action.PlayAction;
import com.vmware.common.dao.Action;
import com.vmware.common.dto.ActionDTO;

/**
 * Action class implementing {@link Action} interface for a no-op/ignore action.
 * 
 * @author vedanthr
 */
public class IgnoreAction extends PlayAction {

	private static final Logger logger = LogManager.getLogger(IgnoreAction.class);

	/**
	 * Ignores/Discards the incoming stream data.
	 * 
	 * @param actionDTO Configuration Details
	 * @param data Data from stream
	 */
	public boolean doAction(ActionDTO actionDTO, String data) {
		logger.traceEntry(data);
		return false;
	}
}