package com.vmware.common.action;

import com.vmware.common.dto.ActionDTO;

/**
 * Abstract class to be extended by each action type
 * 
 * @author vedanthr
 */
public abstract class PlayAction implements ActionInterface {

	public boolean doAction(ActionDTO actionDTO, String data) {
		return true;
	}
}
